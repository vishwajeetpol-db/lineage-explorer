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
import threading
from datetime import datetime, timezone
from typing import Callable, TypeVar
from cachetools import TTLCache
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

T = TypeVar("T")

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
# TTL cache with request coalescing (single-flight pattern).
#
# Backed by cachetools.TTLCache (LRU + TTL, memory-sized via getsizeof).
# Single-flight uses a per-key threading.Lock with double-checked reads:
# when N threads race on an empty key, N-1 block on the lock, and after
# the leader populates the cache they each find the value on re-check.
# ---------------------------------------------------------------------------

CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "28800"))  # default 8 hours
CACHE_MAX_ENTRIES = int(os.environ.get("CACHE_MAX_ENTRIES", "20000"))  # secondary safety valve
CACHE_MAX_MEMORY_MB = int(os.environ.get("CACHE_MAX_MEMORY_MB", "250"))  # primary limit
SQL_WAIT_TIMEOUT = os.environ.get("SQL_WAIT_TIMEOUT", "50s")  # max 50s per Databricks API limit (0s or 5-50s)

_CACHE_MAX_BYTES = CACHE_MAX_MEMORY_MB * 1024 * 1024


class _CacheEntry:
    __slots__ = ("value", "size_bytes", "created_at", "last_accessed")

    def __init__(self, value: object, size_bytes: int):
        self.value = value
        self.size_bytes = size_bytes
        now = time.time()
        self.created_at = now
        self.last_accessed = now


def _estimate_value_size(val: object) -> int:
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


def _entry_size(entry: _CacheEntry) -> int:
    return max(1, entry.size_bytes)


# Memory-bounded TTL+LRU cache. cachetools auto-evicts LRU when currsize > maxsize.
_cache: TTLCache[str, _CacheEntry] = TTLCache(
    maxsize=_CACHE_MAX_BYTES,
    ttl=CACHE_TTL_SECONDS,
    getsizeof=_entry_size,
)
_cache_lock = threading.RLock()

# Per-key locks for single-flight. Reused across calls, created lazily.
_keyed_locks: dict[str, threading.Lock] = {}
_keyed_locks_guard = threading.Lock()


def _get_keyed_lock(key: str) -> threading.Lock:
    with _keyed_locks_guard:
        lock = _keyed_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _keyed_locks[key] = lock
        return lock


def _cache_get(key: str):
    """Return cached value if present and fresh; None otherwise. Promotes LRU + updates last_accessed."""
    with _cache_lock:
        try:
            entry = _cache[key]  # __getitem__ bumps LRU + checks TTL expiry
        except KeyError:
            return None
        entry.last_accessed = time.time()
        return entry.value


def _cache_get_ts(key: str) -> float | None:
    """Return created_at timestamp of a cache entry, or None."""
    with _cache_lock:
        try:
            return _cache[key].created_at
        except KeyError:
            return None


def _cache_set(key: str, val: object) -> None:
    """Store a value. TTLCache handles memory-based LRU eviction; we additionally
    enforce the entry-count cap as a safety valve."""
    entry = _CacheEntry(val, _estimate_value_size(val))
    with _cache_lock:
        _cache[key] = entry
        while len(_cache) > CACHE_MAX_ENTRIES:
            try:
                evicted_key, evicted = _cache.popitem()
                logger.info(
                    f"Cache count-cap eviction: {evicted_key} "
                    f"({evicted.size_bytes / 1024:.1f}KB freed)"
                )
            except KeyError:
                break


def _cached_fetch(key: str, fetcher: Callable[[], T], skip_cache: bool = False) -> T:
    """Single-flight TTL cache helper. Double-checked locking: concurrent callers
    on the same key serialize on a per-key lock, and all but the leader find the
    value already cached on re-check."""
    if not skip_cache:
        cached = _cache_get(key)
        if cached is not None:
            return cached  # type: ignore[return-value]

    lock = _get_keyed_lock(key)
    with lock:
        if not skip_cache:
            cached = _cache_get(key)
            if cached is not None:
                return cached  # type: ignore[return-value]
        result = fetcher()
        _cache_set(key, result)
        return result


def invalidate_cache(prefix: str = "") -> None:
    """Clear all cache entries, or only those matching a prefix."""
    with _cache_lock:
        if not prefix:
            _cache.clear()
        else:
            for k in list(_cache.keys()):
                if k.startswith(prefix):
                    del _cache[k]


def evict_cache_entry(key: str) -> bool:
    """Evict a specific cache entry by key. Returns True if found and evicted."""
    with _cache_lock:
        if key in _cache:
            del _cache[key]
            return True
        return False


def get_cache_snapshot() -> tuple[list[tuple[str, float, float, int]], int, list[str]]:
    """Return cache metadata snapshot for admin dashboard.
    Returns: [(key, created, last_accessed, size_bytes), ...], total_bytes, inflight_keys"""
    with _cache_lock:
        entries = [
            (k, e.created_at, e.last_accessed, e.size_bytes)
            for k, e in _cache.items()
        ]
        total_bytes = _cache.currsize
    with _keyed_locks_guard:
        inflight_keys = [k for k, lock in _keyed_locks.items() if lock.locked()]
    return entries, total_bytes, inflight_keys

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

    def _fetch() -> list[str]:
        client = _get_client()
        skip = {"system", "__databricks_internal"}
        try:
            rows = _execute_sql(client, "SHOW CATALOGS")
            return sorted([r["catalog"] for r in rows if r["catalog"] not in skip])
        except Exception as e:
            logger.error(f"SHOW CATALOGS failed: {e}")
            return []

    # Empty results bypass caching (same as before): retry on next call.
    lock = _get_keyed_lock(cache_key)
    with lock:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        result = _fetch()
        if result:
            _cache_set(cache_key, result)
        return result


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

    def _fetch() -> list[dict]:
        client = _get_client()
        catalogs = list_catalogs()
        tables: list[dict] = []
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
        return tables

    # Never cache empty results (retry on next call).
    lock = _get_keyed_lock(cache_key)
    with lock:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        tables = _fetch()
        if tables:
            _cache_set(cache_key, tables)
        return tables


def list_schemas(catalog: str) -> list[str]:
    """List schemas via SHOW SCHEMAS SQL. Coalesced + cached."""
    cache_key = f"schemas:{catalog}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    def _fetch() -> list[str]:
        client = _get_client()
        skip = {"information_schema", "default"}
        try:
            rows = _execute_sql(client, f"SHOW SCHEMAS IN `{catalog}`", catalog=catalog)
            return sorted([r["databaseName"] for r in rows if r["databaseName"] not in skip])
        except Exception as e:
            logger.error(f"SHOW SCHEMAS failed for {catalog}: {e}")
            return []

    lock = _get_keyed_lock(cache_key)
    with lock:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        result = _fetch()
        if result:
            _cache_set(cache_key, result)
        return result


def _wrap_with_cache_metadata(
    result: LineageResponse,
    cache_key: str,
    from_cache: bool,
    fetch_ms: int | None = None,
) -> LineageResponse:
    """Return a copy of the response with cache metadata attached.

    Uses model_copy so the cached object stays immutable — concurrent requests
    can't observe a half-updated response, and there's no shared-reference drift
    between what's in the cache and what goes out on the wire.
    """
    updates: dict = {}
    cache_ts = _cache_get_ts(cache_key)
    if cache_ts is not None:
        updates["cached"] = from_cache
        updates["cached_at"] = datetime.fromtimestamp(cache_ts, tz=timezone.utc).isoformat()
        updates["cache_expires_at"] = datetime.fromtimestamp(
            cache_ts + CACHE_TTL_SECONDS, tz=timezone.utc
        ).isoformat()
    if fetch_ms is not None:
        updates["fetch_duration_ms"] = fetch_ms
    return result.model_copy(update=updates) if updates else result


def get_table_lineage(catalog: str, schema: str, skip_cache: bool = False) -> LineageResponse:
    cache_key = f"lineage:{catalog}.{schema}"

    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return _wrap_with_cache_metadata(cached, cache_key, from_cache=True, fetch_ms=0)

    lock = _get_keyed_lock(cache_key)
    with lock:
        if not skip_cache:
            cached = _cache_get(cache_key)
            if cached is not None:
                return _wrap_with_cache_metadata(cached, cache_key, from_cache=True, fetch_ms=0)

        fetch_start = time.time()
        result = _fetch_table_lineage(catalog, schema, cache_key)
        fetch_ms = int((time.time() - fetch_start) * 1000)
        _cache_set(cache_key, result)
        return _wrap_with_cache_metadata(result, cache_key, from_cache=False, fetch_ms=fetch_ms)


def _parse_lineage_ref(table_full_name: str | None, path: str | None, ref_type: str | None) -> tuple[str | None, str | None]:
    """Parse a lineage source/target into (node_id, node_type).

    Returns a stable node ID and a type string suitable for TableNode.table_type.
    Handles tables, views, streaming tables, volumes (/Volumes/...), and cloud paths (s3://).
    """
    if table_full_name:
        # Map system lineage types to display types
        type_map = {
            "TABLE": "TABLE",
            "VIEW": "VIEW",
            "MATERIALIZED_VIEW": "MATERIALIZED_VIEW",
            "STREAMING_TABLE": "STREAMING_TABLE",
        }
        return table_full_name, type_map.get(ref_type, ref_type or "TABLE")

    if path:
        # Volume path: /Volumes/catalog/schema/volume_name/...
        if path.startswith("/Volumes/"):
            parts = path.split("/")
            if len(parts) >= 5:
                vol_id = f"{parts[2]}.{parts[3]}.{parts[4]}"
                return vol_id, "VOLUME"
            return f"volume:{path}", "VOLUME"

        # Cloud storage path: s3://bucket/..., abfss://container@account/...
        if "://" in path:
            proto, rest = path.split("://", 1)
            bucket = rest.split("/")[0]
            return f"path:{proto}://{bucket}", "PATH"

        # Other path
        return f"path:{path[:80]}", "PATH"

    return None, None


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

    # Get lineage edges from system tables (with entity info for pipeline nodes).
    # Includes PATH entries (volumes, cloud storage) alongside table references.
    lineage_sql = f"""
    SELECT
        source_table_full_name,
        target_table_full_name,
        source_type,
        target_type,
        source_path,
        target_path,
        entity_type,
        entity_id,
        event_time,
        created_by
    FROM system.access.table_lineage
    WHERE (
        (target_table_catalog = '{catalog}' AND target_table_schema = '{schema}')
        OR
        (source_table_catalog = '{catalog}' AND source_table_schema = '{schema}')
        OR
        (source_path LIKE '/Volumes/{catalog}/{schema}/%')
        OR
        (target_path LIKE '/Volumes/{catalog}/{schema}/%')
    )
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

    # Group lineage rows by entity and build entity nodes + routed edges.
    # Two-pass approach: collect ALL entity rows first (without per-row local
    # filtering), then prune entities that don't touch any local table. This
    # ensures upstream (left-side) tables from other schemas are included when
    # the entity also writes to a local table.
    entity_map: dict[str, dict] = {}  # entity_key → {type, id, sources, targets, last_run, owner}
    direct_pairs: set[tuple[str, str]] = set()  # (src, tgt) for rows with no entity

    # Track external nodes (cross-schema tables, volumes, paths) that need stub nodes
    external_tables: set[str] = set()
    # Track the type of each node for proper rendering (VOLUME, PATH, STREAMING_TABLE, etc.)
    external_node_types: dict[str, str] = {}  # node_id → display type

    for row in lineage_rows:
        # Parse source and target using _parse_lineage_ref which handles
        # tables, volumes (/Volumes/...), and cloud paths (s3://...)
        src, src_type = _parse_lineage_ref(
            row.get("source_table_full_name"),
            row.get("source_path"),
            row.get("source_type"),
        )
        tgt, tgt_type = _parse_lineage_ref(
            row.get("target_table_full_name"),
            row.get("target_path"),
            row.get("target_type"),
        )
        etype = row.get("entity_type")
        eid = row.get("entity_id")

        # Track types for external nodes
        if src and src not in schema_tables and src_type:
            external_node_types[src] = src_type
        if tgt and tgt not in schema_tables and tgt_type:
            external_node_types[tgt] = tgt_type

        # Entity-mediated rows: collect ALL without filtering — pruned below
        if etype and eid:
            entity_key = f"entity:{etype}:{eid}"
            if entity_key not in entity_map:
                entity_map[entity_key] = {
                    "type": etype, "id": eid,
                    "sources": set(), "targets": set(),
                    "last_run": None, "owner": None,
                }
            if src:
                entity_map[entity_key]["sources"].add(src)
            if tgt:
                entity_map[entity_key]["targets"].add(tgt)
            evt = row.get("event_time")
            if evt and (entity_map[entity_key]["last_run"] is None or evt > entity_map[entity_key]["last_run"]):
                entity_map[entity_key]["last_run"] = evt
            owner = row.get("created_by")
            if owner:
                entity_map[entity_key]["owner"] = owner
            continue

        # Direct pair (no entity) — filter per-row: at least one side must be local
        src_local = src in schema_tables if src else False
        tgt_local = tgt in schema_tables if tgt else False
        if not src_local and not tgt_local:
            continue

        if src and not src_local:
            external_tables.add(src)
        if tgt and not tgt_local:
            external_tables.add(tgt)

        if src and tgt:
            direct_pairs.add((src, tgt))

    # Prune entities that don't touch any local table
    pruned_entity_map: dict[str, dict] = {}
    for entity_key, info in entity_map.items():
        touches_local = (
            any(s in schema_tables for s in info["sources"])
            or any(t in schema_tables for t in info["targets"])
        )
        if not touches_local:
            continue
        pruned_entity_map[entity_key] = info
    entity_map = pruned_entity_map

    # Follow-up query: fetch COMPLETE lineage for discovered entities.
    # The initial query only returns rows where source OR target is in our schema,
    # but an entity may write to tables in OTHER schemas (cross-schema targets).
    # Without this, pipelines that read from our schema but write elsewhere show
    # no outward edges.
    if entity_map:
        entity_ids = [info["id"] for info in entity_map.values()]
        eid_list = ",".join(f"'{eid}'" for eid in entity_ids)
        followup_sql = f"""
        SELECT
            source_table_full_name,
            target_table_full_name,
            source_type,
            target_type,
            source_path,
            target_path,
            entity_type,
            entity_id,
            event_time,
            created_by
        FROM system.access.table_lineage
        WHERE entity_id IN ({eid_list})
        AND event_time > current_date() - INTERVAL 90 DAYS
        """
        try:
            followup_rows = _execute_sql(client, followup_sql)
            for row in followup_rows:
                src, src_type = _parse_lineage_ref(
                    row.get("source_table_full_name"),
                    row.get("source_path"),
                    row.get("source_type"),
                )
                tgt, tgt_type = _parse_lineage_ref(
                    row.get("target_table_full_name"),
                    row.get("target_path"),
                    row.get("target_type"),
                )
                etype = row.get("entity_type")
                eid = row.get("entity_id")
                if not etype or not eid:
                    continue
                entity_key = f"entity:{etype}:{eid}"
                if entity_key not in entity_map:
                    continue  # skip entities that were pruned
                if src:
                    entity_map[entity_key]["sources"].add(src)
                    if src not in schema_tables and src_type:
                        external_node_types[src] = src_type
                if tgt:
                    entity_map[entity_key]["targets"].add(tgt)
                    if tgt not in schema_tables and tgt_type:
                        external_node_types[tgt] = tgt_type
                evt = row.get("event_time")
                if evt and (entity_map[entity_key]["last_run"] is None or evt > entity_map[entity_key]["last_run"]):
                    entity_map[entity_key]["last_run"] = evt
                owner = row.get("created_by")
                if owner:
                    entity_map[entity_key]["owner"] = owner
        except Exception as e:
            logger.warning(f"Entity follow-up lineage query failed: {e}")

    # Track external tables from all entity sources/targets
    for info in entity_map.values():
        for s in info["sources"]:
            if s not in schema_tables:
                external_tables.add(s)
        for t in info["targets"]:
            if t not in schema_tables:
                external_tables.add(t)

    # Create stub nodes for cross-schema/cross-catalog tables, volumes, and paths.
    # Group 3-part-name tables by catalog.schema for batch column fetching.
    # Skip column fetch for VOLUME and PATH types (they don't have information_schema).
    non_table_types = {"VOLUME", "PATH"}
    ext_schema_groups: dict[tuple[str, str], list[str]] = {}
    for ext_table in external_tables:
        node_type = external_node_types.get(ext_table, "TABLE")
        if node_type in non_table_types:
            continue  # no columns to fetch for volumes/paths
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
            node_type = external_node_types.get(ext_table, "EXTERNAL_LINEAGE")

            # Determine display name and comment based on node type
            if node_type == "VOLUME":
                display_name = parts[-1] if parts else ext_table
                comment = f"Volume in {'.'.join(parts[:2]) if len(parts) >= 2 else 'external'}"
            elif node_type == "PATH":
                # Cloud storage: strip path: prefix for display
                raw = ext_table.removeprefix("path:")
                display_name = raw.split("://", 1)[-1].split("/")[0] if "://" in raw else raw
                comment = f"External storage: {raw}"
            else:
                display_name = parts[-1] if parts else ext_table
                comment = f"Cross-schema reference from {'.'.join(parts[:2]) if len(parts) >= 2 else 'external'}"

            nodes_map[ext_table] = TableNode(
                id=ext_table,
                name=display_name,
                full_name=ext_table,
                table_type=node_type,
                owner=None,
                comment=comment,
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

    # Fetch compute costs from system.billing (30-day window).
    # Covers both JOB entities (usage_metadata.job_id) and PIPELINE entities
    # (usage_metadata.dlt_pipeline_id). Classic compute is excluded by SKU filter.
    # Cost = DBUs * list price per DBU (per official Databricks docs).
    #   https://docs.databricks.com/aws/en/admin/usage/system-tables
    #
    # Performance: list price is cached globally (24h TTL) — only the fast
    # DBU aggregation queries run per schema load. Entire result is then cached
    # with the lineage response — zero extra queries on repeat visits.
    price_per_dbu = _get_serverless_price(client)

    # Job costs (serverless)
    job_ids = [info["id"] for info in entity_map.values() if info["type"] == "JOB"]
    if job_ids and price_per_dbu > 0:
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

    # Pipeline (DLT) costs — uses usage_metadata.dlt_pipeline_id
    pipeline_ids = [info["id"] for info in entity_map.values() if info["type"] == "PIPELINE"]
    if pipeline_ids and price_per_dbu > 0:
        pipeline_id_list = ",".join(f"'{pid}'" for pid in pipeline_ids)
        dlt_dbu_sql = f"""
        SELECT
            usage_metadata.dlt_pipeline_id AS pipeline_id,
            SUM(usage_quantity) AS total_dbu
        FROM system.billing.usage
        WHERE usage_metadata.dlt_pipeline_id IN ({pipeline_id_list})
            AND usage_date > current_date() - INTERVAL 30 DAYS
        GROUP BY usage_metadata.dlt_pipeline_id
        """
        try:
            dlt_rows = _execute_sql(client, dlt_dbu_sql)
            cost_by_pipeline: dict[str, float] = {}
            for cr in dlt_rows:
                dbu = float(cr["total_dbu"])
                cost_by_pipeline[str(cr["pipeline_id"])] = round(dbu * price_per_dbu, 2)

            for entity_key, info in entity_map.items():
                if info["type"] == "PIPELINE" and info["id"] in cost_by_pipeline:
                    nodes_map[entity_key].cost_usd = cost_by_pipeline[info["id"]]
        except Exception as e:
            logger.warning(f"DLT pipeline cost query failed (ensure SELECT on system.billing is granted): {e}")

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
    # Caching of the top-level lineage response is handled by get_table_lineage.
    return result


def resolve_entity_name(entity_type: str, entity_id: str) -> dict:
    """Resolve an entity ID to display name + metadata via system tables. Coalesced + cached."""
    cache_key = f"entity_name:{entity_type}:{entity_id}"

    def _fetch() -> dict:
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
                if "/" in entity_id:
                    result["name"] = entity_id.split("/")[-1]
                else:
                    # Numeric workspace object ID — resolve via audit log
                    nb_rows = _execute_sql(client, f"""
                        SELECT request_params['path'] AS path
                        FROM system.access.audit
                        WHERE request_params['notebookId'] = '{entity_id}'
                          AND request_params['path'] IS NOT NULL
                        LIMIT 1
                    """)
                    if nb_rows and nb_rows[0].get("path"):
                        result["name"] = nb_rows[0]["path"].rsplit("/", 1)[-1]
                    else:
                        result["name"] = f"Notebook {entity_id[:12]}"
        except Exception as e:
            logger.warning(f"Failed to resolve {entity_type} {entity_id}: {e}")
        return result

    return _cached_fetch(cache_key, _fetch)


def get_columns(catalog: str, schema: str, table: str, skip_cache: bool = False) -> list[dict]:
    """Lazy column loader — returns columns for a single table (cache-first, coalesced)."""
    cache_key = f"columns:{catalog}.{schema}.{table}"

    def _fetch() -> list[dict]:
        client = _get_client()
        sql = f"""
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM `{catalog}`.information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        rows = _execute_sql(client, sql, catalog=catalog)
        return [
            {"name": r["column_name"], "type": r["data_type"], "nullable": r["is_nullable"] == "YES"}
            for r in rows
        ]

    return _cached_fetch(cache_key, _fetch, skip_cache=skip_cache)


def get_schema_column_lineage(catalog: str, schema: str, skip_cache: bool = False) -> ColumnLineageResponse:
    """All column lineage for a schema from system.access.column_lineage.

    Returns every column-level edge within the schema — cached once, shared
    across all column clicks. The frontend does transitive traversal on these
    real UC edges (not heuristic name matching).
    """
    cache_key = f"col_lineage:{catalog}.{schema}"

    def _fetch() -> ColumnLineageResponse:
        client = _get_client()
        rows: list[dict] = []
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
            AND source_table_full_name != target_table_full_name
            AND source_column_name IS NOT NULL
            AND target_column_name IS NOT NULL
            AND event_time > current_date() - INTERVAL 90 DAYS
            LIMIT 50000
            """
            rows = _execute_sql(client, sql)
        except Exception as e:
            logger.warning(f"Schema column lineage query failed: {e}")

        edges = [
            ColumnLineageEdge(
                source_table=row["source_table_full_name"],
                source_column=row["source_column_name"],
                target_table=row["target_table_full_name"],
                target_column=row["target_column_name"],
            )
            for row in rows
        ]
        return ColumnLineageResponse(edges=edges)

    return _cached_fetch(cache_key, _fetch, skip_cache=skip_cache)


def get_column_lineage(catalog: str, schema: str, table: str, column: str, skip_cache: bool = False) -> ColumnLineageResponse:
    """Column lineage for a specific table+column. Delegates to schema-level cache."""
    all_edges = get_schema_column_lineage(catalog, schema, skip_cache)
    full_table = f"{catalog}.{schema}.{table}"
    filtered = [e for e in all_edges.edges
                if (e.source_table == full_table and e.source_column == column)
                or (e.target_table == full_table and e.target_column == column)]
    return ColumnLineageResponse(edges=filtered)


