"""
Lineage service — queries Unity Catalog metadata to build lineage graphs.

Minimum SPN privileges required (no SELECT needed):
  - USE CATALOG on target catalog
  - BROWSE on target catalog
  - USE SCHEMA on target schema(s)
  - CAN_USE on the SQL warehouse

With these privileges, the service can:
  - List catalogs/schemas (information_schema, visible via USE CATALOG)
  - List tables and columns (information_schema, visible via BROWSE)
  - Infer lineage via naming conventions (raw_* -> cleaned_*) and column overlap

Optional privileges that improve lineage quality:
  - SELECT on schema: enables view definition parsing (view_definition visible)
  - SELECT on system.access: enables real lineage from system.access.table_lineage
  - SELECT on system.query: enables query history parsing for CTAS lineage

All optional queries are wrapped in try/except — the app degrades gracefully.
"""

import os
import time
import logging
import random
import threading
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from backend.models import (
    TableNode,
    LineageEdge,
    ColumnLineageEdge,
    LineageResponse,
    ColumnLineageResponse,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton WorkspaceClient — avoids per-request auth handshake overhead
# ---------------------------------------------------------------------------
_client_instance: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _client_instance
    if _client_instance is None:
        _client_instance = WorkspaceClient()
    return _client_instance


# ---------------------------------------------------------------------------
# TTL cache with request coalescing (single-flight pattern)
#
# Solves the thundering herd problem: if 4,000 users hit "Generate Lineage"
# simultaneously on an empty cache, only ONE DBSQL query fires. The other
# 3,999 requests wait on a threading.Event and receive the same result.
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, object]] = {}
_cache_lock = threading.Lock()
_inflight: dict[str, threading.Event] = {}  # keys currently being fetched
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "28800"))  # default 8 hours
SQL_WAIT_TIMEOUT = os.environ.get("SQL_WAIT_TIMEOUT", "50s")  # max 50s per Databricks API limit (0s or 5-50s)
QUERY_HISTORY_DAYS = int(os.environ.get("QUERY_HISTORY_DAYS", "7"))  # lookback window for lineage inference
QUERY_HISTORY_LIMIT = int(os.environ.get("QUERY_HISTORY_LIMIT", "200"))  # max query history rows to scan


def _cache_get(key: str):
    """Return cached value if present and not expired, else None."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        ts, val = entry
        if time.time() - ts > CACHE_TTL_SECONDS:
            del _cache[key]
            return None
        return val


def _cache_set(key: str, val: object):
    with _cache_lock:
        _cache[key] = (time.time(), val)


def _cache_acquire(key: str) -> bool:
    """Try to become the fetcher for this key. Returns True if we are the leader.
    If False, another thread is already fetching — caller should wait then read cache."""
    with _cache_lock:
        if key in _inflight:
            return False
        _inflight[key] = threading.Event()
        return True


def _cache_wait(key: str, timeout: float = 120) -> object | None:
    """Wait for the leader thread to finish fetching, then return the cached value."""
    jitter = random.uniform(0, 10)  # spread wakeups over 10s window
    with _cache_lock:
        event = _inflight.get(key)
    if event is None:
        return _cache_get(key)
    event.wait(timeout=timeout + jitter)
    return _cache_get(key)


def _cache_release(key: str):
    """Signal all waiting threads that the fetch is done."""
    with _cache_lock:
        event = _inflight.pop(key, None)
    if event:
        event.set()


def invalidate_cache(prefix: str = ""):
    """Clear all cache entries, or only those matching a prefix."""
    with _cache_lock:
        if not prefix:
            _cache.clear()
        else:
            for k in list(_cache):
                if k.startswith(prefix):
                    del _cache[k]


def _execute_sql(client: WorkspaceClient, sql: str, catalog: str = None) -> list[dict]:
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    # Find a warehouse if not set
    if not warehouse_id:
        warehouses = list(client.warehouses.list())
        if warehouses:
            warehouse_id = warehouses[0].id
        else:
            raise RuntimeError("No SQL warehouse available")

    resp = client.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        catalog=catalog,
        wait_timeout=SQL_WAIT_TIMEOUT,
    )

    if resp.status.state == StatementState.FAILED:
        raise RuntimeError(f"SQL failed: {resp.status.error.message if resp.status.error else 'Unknown error'}")

    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL did not complete: {resp.status.state}")

    if not resp.result or not resp.result.data_array:
        return []

    columns = [col.name for col in resp.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in resp.result.data_array]


def list_catalogs() -> list[str]:
    """List catalogs using the Unity Catalog API (no system catalog access needed)."""
    cached = _cache_get("catalogs")
    if cached is not None:
        return cached
    client = _get_client()
    skip = {"system", "__databricks_internal"}
    try:
        catalogs = list(client.catalogs.list())
        result = sorted([c.name for c in catalogs if c.name and c.name not in skip])
    except Exception as e:
        logger.warning(f"UC catalog list API failed, falling back to SQL: {e}")
        rows = _execute_sql(client, "SELECT catalog_name FROM system.information_schema.catalogs ORDER BY catalog_name")
        result = [r["catalog_name"] for r in rows if r["catalog_name"] not in skip]
    _cache_set("catalogs", result)
    return result


def list_schemas(catalog: str) -> list[str]:
    """List schemas using the Unity Catalog API (no system catalog access needed)."""
    cache_key = f"schemas:{catalog}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    client = _get_client()
    skip = {"information_schema", "default"}
    try:
        schemas = list(client.schemas.list(catalog_name=catalog))
        result = sorted([s.name for s in schemas if s.name and s.name not in skip])
    except Exception as e:
        logger.warning(f"UC schema list API failed, falling back to SQL: {e}")
        rows = _execute_sql(
            client,
            f"SELECT schema_name FROM `{catalog}`.information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'default') ORDER BY schema_name",
            catalog=catalog,
        )
        result = [r["schema_name"] for r in rows]
    _cache_set(cache_key, result)
    return result


def _infer_lineage(client: WorkspaceClient, catalog: str, schema: str, schema_tables: set[str]) -> list[dict]:
    """Infer lineage by parsing view definitions and query history when system tables are empty."""
    import re
    inferred = []
    table_names = {t.split(".")[-1].lower(): t for t in schema_tables}

    # 1. Parse view definitions - views reference their source tables directly
    try:
        views_sql = f"""
        SELECT table_name, view_definition
        FROM `{catalog}`.information_schema.views
        WHERE table_schema = '{schema}'
        AND view_definition IS NOT NULL
        """
        view_rows = _execute_sql(client, views_sql, catalog=catalog)
        for vr in view_rows:
            view_full = f"{catalog}.{schema}.{vr['table_name']}"
            defn = (vr.get("view_definition") or "").lower()
            for tname, tfull in table_names.items():
                if tfull == view_full:
                    continue
                # Match table name as whole word (after FROM, JOIN, or fully qualified)
                patterns = [
                    rf'\bfrom\s+[`]?{re.escape(tname)}[`]?\b',
                    rf'\bjoin\s+[`]?{re.escape(tname)}[`]?\b',
                    rf'\bfrom\s+[`]?{re.escape(catalog.lower())}[`]?\.[`]?{re.escape(schema.lower())}[`]?\.[`]?{re.escape(tname)}[`]?',
                    rf'\bjoin\s+[`]?{re.escape(catalog.lower())}[`]?\.[`]?{re.escape(schema.lower())}[`]?\.[`]?{re.escape(tname)}[`]?',
                ]
                if any(re.search(p, defn) for p in patterns):
                    inferred.append({"source_table_full_name": tfull, "target_table_full_name": view_full})
    except Exception as e:
        logger.warning(f"View definition parsing failed: {e}")

    # 2. Parse query history for CTAS / INSERT INTO ... SELECT patterns
    try:
        history_sql = f"""
        SELECT DISTINCT
            statement_text
        FROM system.query.history
        WHERE start_time > DATEADD(DAY, -{QUERY_HISTORY_DAYS}, NOW())
        AND statement_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT')
        AND (
            LOWER(statement_text) LIKE '%{catalog.lower()}.{schema.lower()}%'
            OR LOWER(statement_text) LIKE '%{schema.lower()}.%'
        )
        LIMIT {QUERY_HISTORY_LIMIT}
        """
        history_rows = _execute_sql(client, history_sql)
        for hr in history_rows:
            stmt = (hr.get("statement_text") or "").lower()
            # Find target table (appears after CREATE TABLE or INSERT INTO)
            target_full = None
            for tname, tfull in table_names.items():
                if re.search(rf'\b(create\s+(or\s+replace\s+)?table\s+[`]?({re.escape(catalog.lower())}\.{re.escape(schema.lower())}\.)?{re.escape(tname)}[`]?\b|insert\s+(into|overwrite)\s+[`]?({re.escape(catalog.lower())}\.{re.escape(schema.lower())}\.)?{re.escape(tname)}[`]?\b)', stmt):
                    target_full = tfull
                    break
            if target_full:
                for tname2, tfull2 in table_names.items():
                    if tfull2 == target_full:
                        continue
                    if re.search(rf'\b(from|join)\s+[`]?({re.escape(catalog.lower())}\.{re.escape(schema.lower())}\.)?{re.escape(tname2)}[`]?\b', stmt):
                        inferred.append({"source_table_full_name": tfull2, "target_table_full_name": target_full})
    except Exception as e:
        logger.warning(f"Query history parsing failed: {e}")

    # 3. Column-overlap heuristic: always add naming convention edges for non-view tables
    # since CTAS lineage won't be captured from view definitions
    logger.info("Adding naming convention and column-overlap heuristic edges")
    for tname, tfull in table_names.items():
        # raw_ -> cleaned_ pattern
        if tname.startswith("raw_"):
            suffix = tname[4:]
            cleaned_name = f"cleaned_{suffix}"
            if cleaned_name in table_names:
                inferred.append({"source_table_full_name": tfull, "target_table_full_name": table_names[cleaned_name]})

    # 4. Column overlap: for non-raw/non-cleaned tables, find tables that share
    # significant column names with other tables (indicating JOIN / transformation lineage)
    # We get columns to check overlap
    try:
        cols_sql = f"""
        SELECT table_name, column_name
        FROM `{catalog}`.information_schema.columns
        WHERE table_schema = '{schema}'
        """
        col_rows = _execute_sql(client, cols_sql, catalog=catalog)
        cols_by_table: dict[str, set[str]] = {}
        for cr in col_rows:
            tn = cr["table_name"].lower()
            if tn not in cols_by_table:
                cols_by_table[tn] = set()
            cols_by_table[tn].add(cr["column_name"].lower())

        # For gold/aggregate tables, find which silver tables they likely source from
        # A table is likely a source if >50% of its non-metadata columns appear in the target
        metadata_cols = {"processed_at", "ingested_at", "source_system"}
        non_source_tables = {t for t in table_names if not t.startswith("raw_")}
        source_candidates = {t for t in table_names if t.startswith("cleaned_")}
        gold_tables = {t for t in non_source_tables if not t.startswith("cleaned_") and not t.startswith("vw_")}

        for gold in gold_tables:
            gold_cols = cols_by_table.get(gold, set()) - metadata_cols
            if not gold_cols:
                continue
            for src in source_candidates:
                src_cols = cols_by_table.get(src, set()) - metadata_cols
                if not src_cols:
                    continue
                overlap = gold_cols & src_cols
                # If gold table has >=2 columns from the source (or >30% overlap)
                if len(overlap) >= 2 or (src_cols and len(overlap) / len(src_cols) > 0.3):
                    inferred.append({"source_table_full_name": table_names[src], "target_table_full_name": table_names[gold]})

        # For executive_summary type tables, check overlap with gold tables
        for gold in gold_tables:
            gold_cols = cols_by_table.get(gold, set()) - metadata_cols
            for other_gold in gold_tables:
                if other_gold == gold:
                    continue
                other_cols = cols_by_table.get(other_gold, set()) - metadata_cols
                overlap = gold_cols & other_cols
                if len(overlap) >= 3:
                    # The table with more columns is likely the target (aggregate)
                    if len(cols_by_table.get(gold, set())) > len(cols_by_table.get(other_gold, set())):
                        inferred.append({"source_table_full_name": table_names[other_gold], "target_table_full_name": table_names[gold]})
                    elif len(cols_by_table.get(other_gold, set())) > len(cols_by_table.get(gold, set())):
                        inferred.append({"source_table_full_name": table_names[gold], "target_table_full_name": table_names[other_gold]})
    except Exception as e:
        logger.warning(f"Column overlap heuristic failed: {e}")

    # Deduplicate
    seen = set()
    deduped = []
    for edge in inferred:
        key = (edge["source_table_full_name"], edge["target_table_full_name"])
        if key not in seen:
            seen.add(key)
            deduped.append(edge)

    logger.info(f"Inferred {len(deduped)} lineage edges")
    return deduped


def get_table_lineage(catalog: str, schema: str, skip_cache: bool = False) -> LineageResponse:
    cache_key = f"lineage:{catalog}.{schema}"

    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        # Single-flight: if another thread is already fetching this key, wait for it
        if not _cache_acquire(cache_key):
            logger.info(f"Request coalescing: waiting for in-flight fetch of {cache_key}")
            result = _cache_wait(cache_key)
            if result is not None:
                return result
            # Leader failed — backoff before retrying to avoid stampede
            time.sleep(random.uniform(0.05, 0.5))
            if not _cache_acquire(cache_key):
                result = _cache_wait(cache_key)
                if result is not None:
                    return result
                # Still no luck — proceed solo (safe: just a redundant query)

    try:
        result = _fetch_table_lineage(catalog, schema, cache_key)
        return result
    finally:
        _cache_release(cache_key)


def _fetch_table_lineage(catalog: str, schema: str, cache_key: str) -> LineageResponse:
    """Actual DBSQL fetch — called by at most one thread per cache key at a time."""
    client = _get_client()
    full_schema = f"{catalog}.{schema}"

    # Get all tables/views in the schema
    tables_sql = f"""
    SELECT
        table_name,
        table_type,
        table_owner,
        comment,
        created,
        last_altered
    FROM `{catalog}`.information_schema.tables
    WHERE table_schema = '{schema}'
    ORDER BY table_name
    """
    table_rows = _execute_sql(client, tables_sql, catalog=catalog)

    # Get columns for all tables (needed for lineage inference heuristics)
    columns_sql = f"""
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable,
        ordinal_position
    FROM `{catalog}`.information_schema.columns
    WHERE table_schema = '{schema}'
    ORDER BY table_name, ordinal_position
    """
    column_rows = _execute_sql(client, columns_sql, catalog=catalog)

    # Group columns by table
    columns_by_table: dict[str, list[dict]] = {}
    for col in column_rows:
        tname = col["table_name"]
        if tname not in columns_by_table:
            columns_by_table[tname] = []
        columns_by_table[tname].append({
            "name": col["column_name"],
            "type": col["data_type"],
            "nullable": col["is_nullable"] == "YES",
        })

    # Cache columns separately for the lazy /api/columns endpoint
    for tname, cols in columns_by_table.items():
        _cache_set(f"columns:{catalog}.{schema}.{tname}", cols)

    # Pre-build schema_tables set for lineage filtering
    schema_tables = set()
    for t in table_rows:
        schema_tables.add(f"{catalog}.{schema}.{t['table_name']}")

    # Get lineage edges from system tables
    lineage_sql = f"""
    SELECT DISTINCT
        source_table_full_name,
        target_table_full_name
    FROM system.access.table_lineage
    WHERE (
        (target_table_catalog = '{catalog}' AND target_table_schema = '{schema}')
        OR
        (source_table_catalog = '{catalog}' AND source_table_schema = '{schema}')
    )
    AND source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
    """
    try:
        lineage_rows = _execute_sql(client, lineage_sql)
    except Exception as e:
        logger.warning(f"System lineage table query failed: {e}")
        lineage_rows = []

    # Fallback: if system tables are empty, infer lineage from view definitions
    if not lineage_rows:
        logger.info("System lineage empty, inferring from view definitions and query history")
        lineage_rows = _infer_lineage(client, catalog, schema, schema_tables)

    # Build node map
    nodes_map: dict[str, TableNode] = {}
    for t in table_rows:
        table_id = f"{catalog}.{schema}.{t['table_name']}"
        nodes_map[table_id] = TableNode(
            id=table_id,
            name=t["table_name"],
            full_name=table_id,
            table_type=t["table_type"] or "TABLE",
            owner=t.get("table_owner"),
            comment=t.get("comment"),
            columns=columns_by_table.get(t["table_name"], []),
            created_at=t.get("created"),
            updated_at=t.get("last_altered"),
        )

    # Build edges (only between tables in this schema)
    edges = []
    downstream_count: dict[str, int] = {}
    upstream_count: dict[str, int] = {}

    for row in lineage_rows:
        src = row["source_table_full_name"]
        tgt = row["target_table_full_name"]
        if src in schema_tables and tgt in schema_tables:
            edges.append(LineageEdge(source=src, target=tgt))
            downstream_count[src] = downstream_count.get(src, 0) + 1
            upstream_count[tgt] = upstream_count.get(tgt, 0) + 1

    # Update counts on nodes
    for node_id, node in nodes_map.items():
        node.upstream_count = upstream_count.get(node_id, 0)
        node.downstream_count = downstream_count.get(node_id, 0)

    result = LineageResponse(
        nodes=list(nodes_map.values()),
        edges=edges,
    )
    _cache_set(cache_key, result)
    return result


def get_columns(catalog: str, schema: str, table: str, skip_cache: bool = False) -> list[dict]:
    """Lazy column loader — returns columns for a single table (cache-first, coalesced)."""
    cache_key = f"columns:{catalog}.{schema}.{table}"

    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        if not _cache_acquire(cache_key):
            result = _cache_wait(cache_key)
            if result is not None:
                return result
            # Leader failed — backoff before retrying to avoid stampede
            time.sleep(random.uniform(0.05, 0.5))
            if not _cache_acquire(cache_key):
                result = _cache_wait(cache_key)
                if result is not None:
                    return result
                # Still no luck — proceed solo

    try:
        client = _get_client()
        sql = f"""
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM `{catalog}`.information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        rows = _execute_sql(client, sql, catalog=catalog)
        cols = [{"name": r["column_name"], "type": r["data_type"], "nullable": r["is_nullable"] == "YES"} for r in rows]
        _cache_set(cache_key, cols)
        return cols
    finally:
        _cache_release(cache_key)


def get_column_lineage(catalog: str, schema: str, table: str, column: str, skip_cache: bool = False) -> ColumnLineageResponse:
    client = _get_client()

    full_table = f"{catalog}.{schema}.{table}"

    # Try system.access.column_lineage first
    rows = []
    try:
        col_lineage_sql = f"""
        SELECT DISTINCT
            source_table_full_name,
            source_column_name,
            target_table_full_name,
            target_column_name
        FROM system.access.column_lineage
        WHERE (
            (target_table_full_name = '{full_table}' AND target_column_name = '{column}')
            OR
            (source_table_full_name = '{full_table}' AND source_column_name = '{column}')
        )
        AND source_table_full_name IS NOT NULL
        AND target_table_full_name IS NOT NULL
        AND source_column_name IS NOT NULL
        AND target_column_name IS NOT NULL
        """
        rows = _execute_sql(client, col_lineage_sql)
    except Exception as e:
        logger.warning(f"System column lineage query failed: {e}")

    # Fallback: infer column lineage from table-level lineage + matching column names
    if not rows:
        logger.info(f"Inferring column lineage for {full_table}.{column}")
        rows = _infer_column_lineage(client, catalog, schema, table, column)

    edges = []
    for row in rows:
        edges.append(ColumnLineageEdge(
            source_table=row["source_table_full_name"],
            source_column=row["source_column_name"],
            target_table=row["target_table_full_name"],
            target_column=row["target_column_name"],
        ))

    return ColumnLineageResponse(edges=edges)


def _infer_column_lineage(client: WorkspaceClient, catalog: str, schema: str, table: str, column: str) -> list[dict]:
    """Infer column lineage by matching column names across tables connected by table-level lineage."""
    full_table = f"{catalog}.{schema}.{table}"
    inferred = []

    # Get table-level lineage first (reuse the same logic)
    try:
        lineage_resp = get_table_lineage(catalog, schema)
    except Exception:
        return []

    # Find upstream tables (tables that feed into this table)
    upstream_tables = set()
    downstream_tables = set()
    for edge in lineage_resp.edges:
        if edge.target == full_table:
            upstream_tables.add(edge.source)
        if edge.source == full_table:
            downstream_tables.add(edge.target)

    # Get columns for all related tables
    related_tables = upstream_tables | downstream_tables
    columns_by_table: dict[str, set[str]] = {}
    for node in lineage_resp.nodes:
        if node.id in related_tables or node.id == full_table:
            columns_by_table[node.id] = {c["name"].lower() for c in node.columns}

    col_lower = column.lower()

    # For upstream tables: if they have the same column name, it likely flows into this table
    for up_table in upstream_tables:
        up_cols = columns_by_table.get(up_table, set())
        if col_lower in up_cols:
            inferred.append({
                "source_table_full_name": up_table,
                "source_column_name": column,
                "target_table_full_name": full_table,
                "target_column_name": column,
            })

    # For downstream tables: if they have the same column name, this table likely feeds it
    for down_table in downstream_tables:
        down_cols = columns_by_table.get(down_table, set())
        if col_lower in down_cols:
            inferred.append({
                "source_table_full_name": full_table,
                "source_column_name": column,
                "target_table_full_name": down_table,
                "target_column_name": column,
            })

    logger.info(f"Inferred {len(inferred)} column lineage edges for {full_table}.{column}")
    return inferred
