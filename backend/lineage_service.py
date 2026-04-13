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

import json
import os
import sys
import time
import logging
import random
import threading
from collections import OrderedDict
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from backend.models import (
    TableNode,
    EntityNode,
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
# Cache entries: key → (created_at, last_accessed, size_bytes, value)
_cache: OrderedDict[str, tuple[float, float, int, object]] = OrderedDict()
_cache_lock = threading.Lock()
_cache_total_bytes: int = 0  # running total — O(1) memory checks, no re-serialization
_inflight: dict[str, threading.Event] = {}  # keys currently being fetched
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "28800"))  # default 8 hours
CACHE_MAX_ENTRIES = int(os.environ.get("CACHE_MAX_ENTRIES", "20000"))  # secondary safety valve
CACHE_MAX_MEMORY_MB = int(os.environ.get("CACHE_MAX_MEMORY_MB", "250"))  # primary limit
SQL_WAIT_TIMEOUT = os.environ.get("SQL_WAIT_TIMEOUT", "50s")  # max 50s per Databricks API limit (0s or 5-50s)

# Serverless list price per DBU — cached globally (24h TTL, rarely changes)
_serverless_price_per_dbu: float = 0.0
_serverless_price_fetched_at: float = 0.0
_PRICE_CACHE_TTL = 86400  # 24 hours


def _get_serverless_price(client) -> float:
    """Get the current serverless list price per DBU. Cached for 24 hours.
    Called at startup (background) and on lineage load (non-blocking check)."""
    global _serverless_price_per_dbu, _serverless_price_fetched_at
    now = time.time()
    if _serverless_price_per_dbu > 0 and (now - _serverless_price_fetched_at) < _PRICE_CACHE_TTL:
        return _serverless_price_per_dbu
    try:
        price_sql = """
        SELECT pricing.effective_list.default AS price_per_dbu
        FROM system.billing.list_prices
        WHERE sku_name LIKE '%JOBS_SERVERLESS%'
            AND price_end_time IS NULL
        LIMIT 1
        """
        rows = _execute_sql(client, price_sql)
        if rows:
            _serverless_price_per_dbu = float(rows[0]["price_per_dbu"])
            _serverless_price_fetched_at = now
            logger.info(f"Serverless list price: ${_serverless_price_per_dbu}/DBU (cached for 24h)")
    except Exception as e:
        logger.warning(f"list_prices query failed — serverless cost will not be shown: {e}")
    return _serverless_price_per_dbu


def _get_serverless_price_cached() -> float:
    """Non-blocking: return cached price or 0 if not yet fetched.
    Never blocks the lineage request — cost shows on next load after price is cached."""
    if _serverless_price_per_dbu > 0 and (time.time() - _serverless_price_fetched_at) < _PRICE_CACHE_TTL:
        return _serverless_price_per_dbu
    return 0.0


def _estimate_entry_size(val: object) -> int:
    """Estimate memory footprint of a cache value. Computed once at insert time.
    Uses JSON byte length * 2.5 to approximate Python object overhead."""
    try:
        if hasattr(val, 'model_dump'):
            raw = json.dumps(val.model_dump(), default=str)
        elif isinstance(val, (dict, list)):
            raw = json.dumps(val, default=str)
        else:
            return sys.getsizeof(val)
        return int(len(raw.encode('utf-8')) * 2.5)
    except Exception:
        return 1024  # conservative 1KB fallback


def _cache_get(key: str):
    """Return cached value if present and not expired, else None. Promotes key for LRU."""
    global _cache_total_bytes
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        created, _last, size_bytes, val = entry
        if time.time() - created > CACHE_TTL_SECONDS:
            _cache_total_bytes -= size_bytes
            del _cache[key]
            return None
        # LRU: move to end + update last_accessed
        _cache[key] = (created, time.time(), size_bytes, val)
        _cache.move_to_end(key)
        return val


def _cache_get_ts(key: str) -> float | None:
    """Return the created_at timestamp of a cache entry, or None."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        return entry[0]


def _cache_set(key: str, val: object):
    """Set a cache entry. Evicts LRU entries when memory exceeds CACHE_MAX_MEMORY_MB."""
    global _cache_total_bytes
    now = time.time()
    size_bytes = _estimate_entry_size(val)
    max_bytes = CACHE_MAX_MEMORY_MB * 1024 * 1024
    with _cache_lock:
        # If replacing existing entry, subtract old size
        if key in _cache:
            _cache_total_bytes -= _cache[key][2]
            _cache.move_to_end(key)
        _cache[key] = (now, now, size_bytes, val)
        _cache_total_bytes += size_bytes
        # Evict LRU entries if over memory limit or entry count limit
        while len(_cache) > 1 and (_cache_total_bytes > max_bytes or len(_cache) > CACHE_MAX_ENTRIES):
            evicted_key, evicted = _cache.popitem(last=False)
            _cache_total_bytes -= evicted[2]
            logger.info(f"Cache LRU eviction: {evicted_key} ({evicted[2]/1024:.1f}KB freed, total: {_cache_total_bytes/1024/1024:.1f}MB)")


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
    global _cache_total_bytes
    with _cache_lock:
        if not prefix:
            _cache.clear()
            _cache_total_bytes = 0
        else:
            for k in list(_cache):
                if k.startswith(prefix):
                    _cache_total_bytes -= _cache[k][2]
                    del _cache[k]


def evict_cache_entry(key: str) -> bool:
    """Evict a specific cache entry by key. Returns True if found and evicted."""
    global _cache_total_bytes
    with _cache_lock:
        if key in _cache:
            _cache_total_bytes -= _cache[key][2]
            del _cache[key]
            return True
        return False


def get_cache_snapshot() -> tuple[list[tuple[str, float, float, int]], int, list[str]]:
    """Return cache metadata snapshot for admin dashboard.
    Lock held only to copy lightweight metadata — no serialization, no value access.
    Returns: [(key, created, last_accessed, size_bytes), ...], total_bytes, inflight_keys"""
    with _cache_lock:
        entries = [(k, created, last_accessed, size_bytes)
                   for k, (created, last_accessed, size_bytes, _val) in _cache.items()]
        total_bytes = _cache_total_bytes
        inflight_keys = list(_inflight.keys())
    return entries, total_bytes, inflight_keys


def _execute_sql(client: WorkspaceClient, sql: str, catalog: str = None) -> list[dict]:
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        raise RuntimeError("No SQL warehouse available. Set DATABRICKS_WAREHOUSE_ID.")

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
    """List catalogs via SHOW CATALOGS SQL. Coalesced + cached."""
    cache_key = "catalogs"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if not _cache_acquire(cache_key):
        result = _cache_wait(cache_key)
        if result is not None:
            return result

    try:
        client = _get_client()
        skip = {"system", "__databricks_internal"}
        try:
            rows = _execute_sql(client, "SHOW CATALOGS")
            result = sorted([r["catalog"] for r in rows if r["catalog"] not in skip])
        except Exception as e:
            logger.error(f"SHOW CATALOGS failed: {e}")
            result = []
        if result:
            _cache_set(cache_key, result)
        return result
    finally:
        _cache_release(cache_key)


def list_all_tables() -> list[dict]:
    """List all tables across all accessible catalogs via SQL.

    Uses per-catalog information_schema.tables — no UC SDK listing calls.
    The SDK's tables.list() has the include_browse issue: it silently
    returns empty for BROWSE-only access. SQL is authoritative.
    Cached with TTL/LRU + request coalescing. Never caches empty results.
    """
    cache_key = "all_tables"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if not _cache_acquire(cache_key):
        logger.info("Request coalescing: waiting for in-flight fetch of all_tables")
        result = _cache_wait(cache_key)
        if result is not None:
            return result
        time.sleep(random.uniform(0.05, 0.5))
        if not _cache_acquire(cache_key):
            result = _cache_wait(cache_key)
            if result is not None:
                return result

    try:
        client = _get_client()
        catalogs = list_catalogs()
        tables = []

        for cat in catalogs:
            try:
                sql = f"""
                SELECT table_name, table_type, table_schema
                FROM `{cat}`.information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'default')
                ORDER BY table_schema, table_name
                """
                rows = _execute_sql(client, sql, catalog=cat)
                for r in rows:
                    sch = r["table_schema"]
                    name = r["table_name"]
                    tables.append({
                        "name": name,
                        "fqdn": f"{cat}.{sch}.{name}",
                        "catalog": cat,
                        "schema": sch,
                        "table_type": r["table_type"] or "TABLE",
                    })
            except Exception as e:
                logger.warning(f"Failed to list tables in catalog {cat}: {e}")
                continue

        if tables:
            _cache_set(cache_key, tables)
        return tables
    finally:
        _cache_release(cache_key)


def list_schemas(catalog: str) -> list[str]:
    """List schemas via SHOW SCHEMAS SQL. Coalesced + cached."""
    cache_key = f"schemas:{catalog}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if not _cache_acquire(cache_key):
        result = _cache_wait(cache_key)
        if result is not None:
            return result

    try:
        client = _get_client()
        skip = {"information_schema", "default"}
        try:
            rows = _execute_sql(client, f"SHOW SCHEMAS IN `{catalog}`", catalog=catalog)
            result = sorted([r["databaseName"] for r in rows if r["databaseName"] not in skip])
        except Exception as e:
            logger.error(f"SHOW SCHEMAS failed for {catalog}: {e}")
            result = []
        if result:
            _cache_set(cache_key, result)
        return result
    finally:
        _cache_release(cache_key)


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

    # Get lineage edges from system tables (with entity info for pipeline nodes)
    lineage_sql = f"""
    SELECT
        source_table_full_name,
        target_table_full_name,
        entity_type,
        entity_id,
        event_time,
        created_by
    FROM system.access.table_lineage
    WHERE (
        (target_table_catalog = '{catalog}' AND target_table_schema = '{schema}')
        OR
        (source_table_catalog = '{catalog}' AND source_table_schema = '{schema}')
    )
    AND source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
    AND event_time > current_date() - INTERVAL 90 DAYS
    """
    try:
        lineage_rows = _execute_sql(client, lineage_sql)
    except Exception as e:
        logger.warning(f"System lineage table query failed (ensure SELECT on system.access is granted): {e}")
        lineage_rows = []

    # Build table node map
    nodes_map: dict[str, TableNode | EntityNode] = {}
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

    # Group lineage rows by entity and build entity nodes + routed edges
    entity_map: dict[str, dict] = {}  # entity_key → {type, id, sources, targets, last_run, owner}
    direct_pairs: set[tuple[str, str]] = set()  # (src, tgt) for rows with no entity

    # Track cross-schema tables that need stub nodes
    external_tables: set[str] = set()

    for row in lineage_rows:
        src = row["source_table_full_name"]
        tgt = row["target_table_full_name"]

        # At least one side must be in the current schema
        src_local = src in schema_tables
        tgt_local = tgt in schema_tables
        if not src_local and not tgt_local:
            continue

        # Track external (cross-schema/cross-catalog) tables for stub nodes
        if not src_local:
            external_tables.add(src)
        if not tgt_local:
            external_tables.add(tgt)

        etype = row.get("entity_type")
        eid = row.get("entity_id")

        if not etype or not eid:
            direct_pairs.add((src, tgt))
            continue

        entity_key = f"entity:{etype}:{eid}"
        if entity_key not in entity_map:
            entity_map[entity_key] = {
                "type": etype, "id": eid,
                "sources": set(), "targets": set(),
                "last_run": None, "owner": None,
            }
        entity_map[entity_key]["sources"].add(src)
        entity_map[entity_key]["targets"].add(tgt)
        # Track the latest event_time per entity
        evt = row.get("event_time")
        if evt and (entity_map[entity_key]["last_run"] is None or evt > entity_map[entity_key]["last_run"]):
            entity_map[entity_key]["last_run"] = evt
        owner = row.get("created_by")
        if owner:
            entity_map[entity_key]["owner"] = owner

    # Create stub nodes for cross-schema/cross-catalog tables with real column metadata
    # Group external tables by catalog.schema for batch column fetching
    ext_schema_groups: dict[tuple[str, str], list[str]] = {}
    for ext_table in external_tables:
        parts = ext_table.split(".")
        if len(parts) == 3:
            key = (parts[0], parts[1])
            if key not in ext_schema_groups:
                ext_schema_groups[key] = []
            ext_schema_groups[key].append(parts[2])

    # Fetch column metadata for external tables from their information_schema
    ext_columns: dict[str, list[dict]] = {}  # table_fqdn → [{name, type, nullable}]
    for (ext_cat, ext_sch), table_names in ext_schema_groups.items():
        table_list = ",".join(f"'{t}'" for t in table_names)
        col_sql = f"""
        SELECT table_name, column_name, full_data_type, is_nullable
        FROM {ext_cat}.information_schema.columns
        WHERE table_schema = '{ext_sch}' AND table_name IN ({table_list})
        ORDER BY table_name, ordinal_position
        """
        try:
            col_rows = _execute_sql(client, col_sql)
            for cr in col_rows:
                fqdn = f"{ext_cat}.{ext_sch}.{cr['table_name']}"
                if fqdn not in ext_columns:
                    ext_columns[fqdn] = []
                ext_columns[fqdn].append({
                    "name": cr["column_name"],
                    "type": cr["full_data_type"],
                    "nullable": cr.get("is_nullable", "YES") == "YES",
                })
        except Exception as e:
            logger.warning(f"Failed to fetch columns for external tables in {ext_cat}.{ext_sch}: {e}")

    for ext_table in external_tables:
        if ext_table not in nodes_map:
            parts = ext_table.split(".")
            nodes_map[ext_table] = TableNode(
                id=ext_table,
                name=parts[-1] if parts else ext_table,
                full_name=ext_table,
                table_type="EXTERNAL_LINEAGE",
                owner=None,
                comment=f"Cross-schema reference from {'.'.join(parts[:2]) if len(parts) >= 2 else 'external'}",
                columns=ext_columns.get(ext_table, []),
                created_at=None,
                updated_at=None,
            )

    # Create entity nodes
    for entity_key, info in entity_map.items():
        nodes_map[entity_key] = EntityNode(
            id=entity_key,
            entity_type=info["type"],
            entity_id=info["id"],
            last_run=info["last_run"],
            owner=info["owner"],
        )

    # Fetch serverless job costs from system.billing (30-day window).
    # Only for JOB entities — classic compute is excluded by SKU filter.
    # Cost = DBUs * list price per DBU (per official Databricks docs).
    #   https://docs.databricks.com/aws/en/admin/usage/system-tables
    #
    # Performance: list price is cached globally (24h TTL) — only the fast
    # DBU aggregation query runs per schema load. Entire result is then cached
    # with the lineage response — zero extra queries on repeat visits.
    job_ids = [info["id"] for info in entity_map.values() if info["type"] == "JOB"]
    if job_ids:
        # Blocking on first call (once per 24h), instant from cache after that.
        price_per_dbu = _get_serverless_price(client)
        if price_per_dbu > 0:
            job_id_list = ",".join(f"'{jid}'" for jid in job_ids)
            dbu_sql = f"""
            SELECT
                usage_metadata.job_id AS job_id,
                SUM(usage_quantity) AS total_dbu
            FROM system.billing.usage
            WHERE sku_name LIKE '%SERVERLESS%'
                AND usage_metadata.job_id IN ({job_id_list})
                AND usage_date > current_date() - INTERVAL 30 DAYS
            GROUP BY usage_metadata.job_id
            """
            try:
                dbu_rows = _execute_sql(client, dbu_sql)
                cost_by_job: dict[str, float] = {}
                for cr in dbu_rows:
                    dbu = float(cr["total_dbu"])
                    cost_by_job[str(cr["job_id"])] = round(dbu * price_per_dbu, 2)

                for entity_key, info in entity_map.items():
                    if info["type"] == "JOB" and info["id"] in cost_by_job:
                        nodes_map[entity_key].cost_usd = cost_by_job[info["id"]]
            except Exception as e:
                logger.warning(f"Serverless job cost query failed (ensure SELECT on system.billing is granted): {e}")

    # Build edges: routed through entity nodes + direct edges
    edge_set: set[tuple[str, str]] = set()

    for entity_key, info in entity_map.items():
        for src in info["sources"]:
            edge_set.add((src, entity_key))
        for tgt in info["targets"]:
            edge_set.add((entity_key, tgt))

    # Direct edges (no entity info — backward compat)
    # Only add if not already covered by an entity-routed path
    entity_covered = set()
    for info in entity_map.values():
        for src in info["sources"]:
            for tgt in info["targets"]:
                entity_covered.add((src, tgt))

    for src, tgt in direct_pairs:
        if (src, tgt) not in entity_covered:
            edge_set.add((src, tgt))

    edges = [LineageEdge(source=s, target=t) for s, t in edge_set]

    # Calculate upstream/downstream counts for table nodes only
    downstream_count: dict[str, int] = {}
    upstream_count: dict[str, int] = {}
    for s, t in edge_set:
        # Count table-to-table connectivity (skip entity intermediaries)
        if s in schema_tables:
            downstream_count[s] = downstream_count.get(s, 0) + 1
        if t in schema_tables:
            upstream_count[t] = upstream_count.get(t, 0) + 1

    for node_id, node in nodes_map.items():
        if not isinstance(node, TableNode):
            continue
        node.upstream_count = upstream_count.get(node_id, 0)
        node.downstream_count = downstream_count.get(node_id, 0)
        if node.upstream_count == 0 and node.downstream_count == 0:
            node.lineage_status = "orphan"
        elif node.upstream_count == 0:
            node.lineage_status = "root"
        elif node.downstream_count == 0:
            node.lineage_status = "leaf"
        else:
            node.lineage_status = "connected"

    result = LineageResponse(
        nodes=list(nodes_map.values()),
        edges=edges,
    )
    _cache_set(cache_key, result)
    return result


def resolve_entity_name(entity_type: str, entity_id: str) -> dict:
    """Resolve an entity ID to display name + metadata via system tables. Coalesced + cached."""
    cache_key = f"entity_name:{entity_type}:{entity_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if not _cache_acquire(cache_key):
        result = _cache_wait(cache_key)
        if result is not None:
            return result

    client = _get_client()
    result = {"name": f"{entity_type} {entity_id[:12]}"}

    try:
        if entity_type == "JOB":
            rows = _execute_sql(client, f"""
                SELECT name, run_as_user_name, creator_user_name
                FROM system.lakeflow.jobs
                WHERE job_id = '{entity_id}'
                LIMIT 1
            """)
            if rows:
                r = rows[0]
                if r.get("name"):
                    result["name"] = r["name"]
                result["owner"] = r.get("run_as_user_name") or r.get("creator_user_name")
        elif entity_type == "PIPELINE":
            rows = _execute_sql(client, f"""
                SELECT name FROM system.lakeflow.pipelines
                WHERE pipeline_id = '{entity_id}'
                LIMIT 1
            """)
            if rows and rows[0].get("name"):
                result["name"] = rows[0]["name"]
        elif entity_type == "NOTEBOOK":
            result["name"] = entity_id.split("/")[-1] if "/" in entity_id else f"Notebook {entity_id[:12]}"
    except Exception as e:
        logger.warning(f"Failed to resolve {entity_type} {entity_id}: {e}")

    try:
        _cache_set(cache_key, result)
        return result
    finally:
        _cache_release(cache_key)


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


def get_schema_column_lineage(catalog: str, schema: str, skip_cache: bool = False) -> ColumnLineageResponse:
    """All column lineage for a schema from system.access.column_lineage.

    Returns every column-level edge within the schema — cached once, shared
    across all column clicks. The frontend does transitive traversal on these
    real UC edges (not heuristic name matching).
    """
    cache_key = f"col_lineage:{catalog}.{schema}"

    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        if not _cache_acquire(cache_key):
            result = _cache_wait(cache_key)
            if result is not None:
                return result
            time.sleep(random.uniform(0.05, 0.5))
            if not _cache_acquire(cache_key):
                result = _cache_wait(cache_key)
                if result is not None:
                    return result

    try:
        client = _get_client()
        rows = []
        try:
            sql = f"""
            SELECT DISTINCT
                source_table_full_name,
                source_column_name,
                target_table_full_name,
                target_column_name
            FROM system.access.column_lineage
            WHERE (
                (target_table_catalog = '{catalog}' AND target_table_schema = '{schema}')
                OR
                (source_table_catalog = '{catalog}' AND source_table_schema = '{schema}')
            )
            AND source_table_full_name IS NOT NULL
            AND target_table_full_name IS NOT NULL
            AND source_column_name IS NOT NULL
            AND target_column_name IS NOT NULL
            AND event_time > current_date() - INTERVAL 90 DAYS
            LIMIT 50000
            """
            rows = _execute_sql(client, sql)
        except Exception as e:
            logger.warning(f"Schema column lineage query failed: {e}")

        edges = []
        for row in rows:
            edges.append(ColumnLineageEdge(
                source_table=row["source_table_full_name"],
                source_column=row["source_column_name"],
                target_table=row["target_table_full_name"],
                target_column=row["target_column_name"],
            ))

        result = ColumnLineageResponse(edges=edges)
        _cache_set(cache_key, result)
        return result
    finally:
        _cache_release(cache_key)


def get_column_lineage(catalog: str, schema: str, table: str, column: str, skip_cache: bool = False) -> ColumnLineageResponse:
    """Column lineage for a specific table+column. Delegates to schema-level cache."""
    all_edges = get_schema_column_lineage(catalog, schema, skip_cache)
    full_table = f"{catalog}.{schema}.{table}"
    filtered = [e for e in all_edges.edges
                if (e.source_table == full_table and e.source_column == column)
                or (e.target_table == full_table and e.target_column == column)]
    return ColumnLineageResponse(edges=filtered)


