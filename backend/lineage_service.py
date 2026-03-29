"""
Lineage service — queries Unity Catalog system tables to build lineage graphs.

Required SPN privileges:
  - USE CATALOG on target catalog
  - BROWSE on target catalog
  - USE SCHEMA on target schema(s)
  - CAN_USE on the SQL warehouse
  - USE CATALOG on system catalog
  - USE SCHEMA on system.access
  - SELECT on system.access (for table_lineage and column_lineage)

Lineage data comes exclusively from system.access.table_lineage and
system.access.column_lineage — the source of truth captured by Unity Catalog
from actual query execution. No inference, no heuristics, no regex parsing.
"""

import os
import time
import logging
import random
import threading
from collections import OrderedDict
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
_cache: OrderedDict[str, tuple[float, object]] = OrderedDict()
_cache_lock = threading.Lock()
_inflight: dict[str, threading.Event] = {}  # keys currently being fetched
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "28800"))  # default 8 hours
CACHE_MAX_ENTRIES = int(os.environ.get("CACHE_MAX_ENTRIES", "500"))  # LRU eviction threshold
SQL_WAIT_TIMEOUT = os.environ.get("SQL_WAIT_TIMEOUT", "50s")  # max 50s per Databricks API limit (0s or 5-50s)


def _cache_get(key: str):
    """Return cached value if present and not expired, else None. Promotes key for LRU."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        ts, val = entry
        if time.time() - ts > CACHE_TTL_SECONDS:
            del _cache[key]
            return None
        # LRU: move to end (most recently used)
        _cache.move_to_end(key)
        return val


def _cache_get_ts(key: str) -> float | None:
    """Return the timestamp when a cache entry was set, or None."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        return entry[0]


def _cache_set(key: str, val: object):
    """Set a cache entry. Evicts least-recently-used entries if over CACHE_MAX_ENTRIES."""
    with _cache_lock:
        if key in _cache:
            _cache.move_to_end(key)
        _cache[key] = (time.time(), val)
        # Evict LRU entries if cache is too large
        while len(_cache) > CACHE_MAX_ENTRIES:
            evicted_key, _ = _cache.popitem(last=False)
            logger.info(f"Cache LRU eviction: {evicted_key} (size: {len(_cache)})")


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



def _add_cache_metadata(result: LineageResponse, cache_key: str, fetch_ms: int | None = None, from_cache: bool = False) -> LineageResponse:
    """Attach cache metadata to the response."""
    from datetime import datetime, timezone
    cache_ts = _cache_get_ts(cache_key)
    if cache_ts is not None:
        result.cached = from_cache
        result.cached_at = datetime.fromtimestamp(cache_ts, tz=timezone.utc).isoformat()
        expires = cache_ts + CACHE_TTL_SECONDS
        result.cache_expires_at = datetime.fromtimestamp(expires, tz=timezone.utc).isoformat()
    if fetch_ms is not None:
        result.fetch_duration_ms = fetch_ms
    return result


def get_table_lineage(catalog: str, schema: str, skip_cache: bool = False) -> LineageResponse:
    cache_key = f"lineage:{catalog}.{schema}"

    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return _add_cache_metadata(cached, cache_key, fetch_ms=0, from_cache=True)

        # Single-flight: if another thread is already fetching this key, wait for it
        if not _cache_acquire(cache_key):
            logger.info(f"Request coalescing: waiting for in-flight fetch of {cache_key}")
            result = _cache_wait(cache_key)
            if result is not None:
                return _add_cache_metadata(result, cache_key, fetch_ms=0, from_cache=True)
            # Leader failed — backoff before retrying to avoid stampede
            time.sleep(random.uniform(0.05, 0.5))
            if not _cache_acquire(cache_key):
                result = _cache_wait(cache_key)
                if result is not None:
                    return _add_cache_metadata(result, cache_key, fetch_ms=0, from_cache=True)
                # Still no luck — proceed solo (safe: just a redundant query)

    try:
        fetch_start = time.time()
        result = _fetch_table_lineage(catalog, schema, cache_key)
        fetch_ms = int((time.time() - fetch_start) * 1000)
        return _add_cache_metadata(result, cache_key, fetch_ms=fetch_ms, from_cache=False)
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

    # Get columns for all tables
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
        logger.warning(f"System lineage table query failed (ensure SELECT on system.access is granted): {e}")
        lineage_rows = []

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

    edges = []
    for row in rows:
        edges.append(ColumnLineageEdge(
            source_table=row["source_table_full_name"],
            source_column=row["source_column_name"],
            target_table=row["target_table_full_name"],
            target_column=row["target_column_name"],
        ))

    return ColumnLineageResponse(edges=edges)


