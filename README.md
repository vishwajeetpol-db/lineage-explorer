# Lineage Explorer

Interactive data lineage visualization for Databricks Unity Catalog. Explore table dependencies, column-level lineage, and data flow across schemas — all through a polished DAG interface.

![Tech Stack](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white) ![React](https://img.shields.io/badge/React-61DAFB?style=flat&logo=react&logoColor=black) ![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?style=flat&logo=typescript&logoColor=white) ![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)

---

## Features

- **Table Lineage DAG** — Automatic left-to-right layout via ELK.js with animated edge routing
- **Column-Level Lineage** — Expand nodes to see columns, click a column to trace its flow upstream/downstream
- **Live Mode** — Toggle between cached data (instant) and live system table queries
- **Smart Lineage Inference** — When `system.access.table_lineage` is unavailable, lineage is inferred from view definitions, query history, naming conventions (`raw_*` -> `cleaned_*`), and column overlap heuristics
- **Request Coalescing** — Single-flight pattern prevents thundering herd: 4,000 simultaneous requests generate only 1 DBSQL query
- **Staggered Reveal Animation** — Nodes cascade left-to-right after layout, edges appear when both endpoints are visible
- **Search** — Cmd+K to search tables/views
- **Interactive** — Drag nodes, zoom/pan, hover tooltips, node highlighting with upstream/downstream paths
- **Reset Layout** — Re-run ELK and replay reveal animation after dragging nodes

---

## Quick Start: Deploy to Any Workspace (Zero Code Edits)

### Prerequisites

| Requirement | How to Check |
|-------------|-------------|
| Databricks CLI v0.239+ | `databricks --version` |
| CLI authenticated to target workspace | `databricks auth login --profile <name>` |
| SQL Warehouse (serverless or pro) | Note the warehouse ID from UI or `databricks warehouses list` |
| Unity Catalog enabled | At least one catalog with data to explore |
| Node.js 18+ (only if rebuilding frontend) | `node --version` — pre-built `dist/` is committed, so this is optional |

### Step 1: Clone

```bash
git clone <repo-url>
cd lineage-explorer
```

### Step 2: Deploy via DABs (One Command)

```bash
databricks bundle deploy -t dev \
  --profile <your-workspace-profile> \
  --var warehouse_id=<your-warehouse-id>
```

Then start the app:
```bash
databricks bundle run lineage-explorer -t dev --profile <your-workspace-profile>
```

No files to edit. The `--profile` flag selects the workspace from `~/.databrickscfg`, and `--var` provides the warehouse ID.

### Step 3: Grant Permissions to the App's SPN

After deployment, Databricks auto-creates a service principal for the app. Grant it access:

```bash
# Get the app's auto-generated SPN client ID
APP_SPN=$(databricks apps get lineage-explorer-dev --profile <your-profile> -o json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['service_principal_client_id'])")
echo "App SPN: $APP_SPN"
```

**Required grants** (minimum for the app to function):

```sql
-- Replace <catalog>, <schema>, and <app-spn> with actual values
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-spn>`;
GRANT BROWSE ON CATALOG <catalog> TO `<app-spn>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<app-spn>`;
```

**Grant warehouse access** (via API — replace values):

```bash
curl -X PUT "https://<workspace-host>/api/2.0/permissions/sql/warehouses/<warehouse-id>" \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{"access_control_list": [{"service_principal_name": "<app-spn>", "permission_level": "CAN_USE"}]}'
```

Or via UI: **SQL Warehouses > Your Warehouse > Permissions > Add the app SPN with "Can Use"**

### Step 4: Verify

```bash
# Get app URL
databricks apps get lineage-explorer-dev --profile <your-profile> -o json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
```

Open the URL, select a catalog and schema, click "Generate Lineage".

---

## Deploying via CI/CD (Service Principal)

When deploying with an external SPN (not a human user), the **deploying SPN** also needs permissions:

```sql
-- Deploying SPN needs these to validate the app
GRANT USE CATALOG ON CATALOG <catalog> TO `<deploying-spn>`;
GRANT BROWSE ON CATALOG <catalog> TO `<deploying-spn>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<deploying-spn>`;
```

Plus warehouse `CAN_USE` (same API call as above, with the deploying SPN's client_id).

Configure the SPN in `~/.databrickscfg`:

```ini
[my-spn-profile]
host          = https://<workspace>.cloud.databricks.com
client_id     = <spn-client-id>
client_secret = <spn-secret>
auth_type     = oauth-m2m
```

Then deploy:
```bash
databricks bundle deploy -t prod --profile my-spn-profile --var warehouse_id=<wh-id>
databricks bundle run lineage-explorer -t prod --profile my-spn-profile
```

---

## Optional: Enhanced Lineage Privileges

Without optional privileges, the app infers lineage from naming conventions, column overlap, and query history (~60-85% coverage). Adding system table access improves coverage:

```sql
-- Enables view definition parsing (~85-90% coverage)
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<app-spn>`;

-- Enables real Unity Catalog lineage (~100% coverage, requires metastore admin)
GRANT USE CATALOG ON CATALOG system TO `<app-spn>`;
GRANT USE SCHEMA ON SCHEMA system.access TO `<app-spn>`;
GRANT SELECT ON SCHEMA system.access TO `<app-spn>`;
GRANT USE SCHEMA ON SCHEMA system.query TO `<app-spn>`;
GRANT SELECT ON SCHEMA system.query TO `<app-spn>`;
```

| Privileges Granted | Lineage Strategies | Approx Coverage |
|---|---|---|
| Minimum only (no SELECT) | Naming conventions + column overlap | ~60-70% |
| + SELECT on schema | Above + view definition parsing | ~85-90% |
| + SELECT on system tables | Real Unity Catalog lineage | ~100% |

The app **never crashes** regardless of privilege level — all optional queries are wrapped in try/except with graceful fallback.

---

## Complete Permission Reference

### Two SPNs Need Permissions

| SPN | What It Is | When It Exists |
|-----|-----------|---------------|
| **App SPN** | Auto-created by Databricks when the app is deployed | After `bundle deploy` |
| **Deploying SPN** | External SPN used for CI/CD automation | Only when deploying via SPN (not needed for human user deploys) |

### Required Permissions Matrix

| Permission | App SPN | Deploying SPN | How to Grant |
|-----------|---------|--------------|-------------|
| `USE CATALOG` on target catalog | Yes | Yes (if SPN) | `GRANT USE CATALOG ON CATALOG <cat> TO \`<spn>\`` |
| `BROWSE` on target catalog | Yes | Yes (if SPN) | `GRANT BROWSE ON CATALOG <cat> TO \`<spn>\`` |
| `USE SCHEMA` on target schema(s) | Yes | Yes (if SPN) | `GRANT USE SCHEMA ON SCHEMA <cat>.<sch> TO \`<spn>\`` |
| `CAN_USE` on SQL Warehouse | Yes | Yes (if SPN) | Permissions API (PUT) or UI |
| Workspace membership | Auto (app SPN is auto-added) | Must exist in workspace | Add via SCIM API or UI |

### Optional Permissions (Enhanced Lineage)

| Permission | Purpose |
|-----------|---------|
| `SELECT` on target schema | View definition parsing for lineage inference |
| `USE CATALOG` on `system` | Access to system tables |
| `SELECT` on `system.access` | Real table/column lineage from Unity Catalog |
| `SELECT` on `system.query` | Query history parsing for CTAS lineage |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATABRICKS APP                              │
│                                                                     │
│   ┌──────────────────────┐          ┌──────────────────────────┐    │
│   │    FastAPI Backend   │          │     React Frontend       │    │
│   │                      │  JSON    │                          │    │
│   │  Endpoints:          │ ──────►  │  React Flow  (DAG)       │    │
│   │    /api/lineage      │          │  ELK.js     (layout)     │    │
│   │    /api/columns      │          │  Framer     (animation)  │    │
│   │    /api/catalogs     │          │  Zustand    (state)      │    │
│   │    /api/schemas      │          │  Tailwind   (dark UI)    │    │
│   │    /health           │          │                          │    │
│   │                      │          │  Column lineage tracing  │    │
│   │  Middleware:         │          │  ErrorBoundary (guard)   │    │
│   │    Rate limiting     │          └──────────────────────────┘    │
│   │    Input validation  │                                          │
│   │    Error sanitization│                                          │
│   └──────────┬───────────┘                                          │
│              │                                                      │
│              │  In-memory TTL cache (8h)                            │
│              │  Request coalescing (single-flight)                  │
│              │                                                      │
│              ▼                                                      │
│   ┌──────────────────────┐                                          │
│   │   Databricks SDK     │  OAuth auto-injected (no credentials)    │
│   └──────────┬───────────┘                                          │
│              │                                                      │
└──────────────┼──────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       UNITY CATALOG                                 │
│                                                                     │
│   information_schema          system.access          system.query   │
│   ├── tables                  ├── table_lineage      └── history    │
│   ├── columns                 └── column_lineage                    │
│   └── views                                                         │
└─────────────────────────────────────────────────────────────────────┘
```

### Authentication Model

| Method | Env Vars Required | Use Case |
|--------|------------------|----------|
| **Databricks App (auto)** | None — injected automatically | Production: running as a Databricks App |
| **Service Principal (OAuth M2M)** | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | CI/CD, automation, standalone deployment |
| **Personal Access Token** | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | Local development only |

---

## Configuration (Environment Variables)

All configuration is via environment variables. When deploying via DABs, `DATABRICKS_WAREHOUSE_ID` is set automatically from `--var warehouse_id=...`. All others have sensible defaults.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABRICKS_WAREHOUSE_ID` | (auto-detect) | **Required.** SQL warehouse ID for queries |
| `CACHE_TTL_SECONDS` | `28800` (8h) | How long cached lineage data lives |
| `SQL_WAIT_TIMEOUT` | `50s` | SQL statement execution timeout (max 50s per API limit) |
| `QUERY_HISTORY_DAYS` | `7` | How many days of query history to scan for lineage inference |
| `QUERY_HISTORY_LIMIT` | `200` | Max query history rows to scan per schema |
| `RATE_LIMIT_MAX_REQUESTS` | `60` | Max API requests per IP per window |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Rate limit window duration |

To override via DABs, add to the `env` section in `databricks.yml`:
```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: ${var.warehouse_id}
  - name: CACHE_TTL_SECONDS
    value: "3600"
```

---

## Local Development

```bash
# Terminal 1 — Backend
pip install -r requirements.txt
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-pat>"   # or use SP OAuth env vars
export DATABRICKS_WAREHOUSE_ID="<warehouse-id>"
uvicorn backend.main:app --reload --port 8000

# Terminal 2 — Frontend (with hot reload + API proxy to :8000)
cd frontend && npm install && npm run dev
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check — returns status and version |
| `GET` | `/api/catalogs` | List available catalogs |
| `GET` | `/api/schemas?catalog=X` | List schemas in a catalog |
| `GET` | `/api/lineage?catalog=X&schema=Y&live=false` | Get table lineage graph |
| `GET` | `/api/columns?catalog=X&schema=Y&table=Z` | Get columns for a table |
| `GET` | `/api/column-lineage?catalog=X&schema=Y&table=Z&column=W` | Get column-level lineage |
| `POST` | `/api/cache/invalidate` | Clear cache (localhost only) |

All identifier parameters are validated: alphanumeric + underscores, max 255 chars.

---

## Security

| Protection | Description |
|-----------|-------------|
| **Input Validation** | All SQL-interpolated parameters validated against strict regex before use |
| **Path Traversal** | Static file serving resolves paths and verifies they stay within dist directory |
| **Rate Limiting** | Configurable req/min/IP on API endpoints, bounded memory (max 10K tracked IPs with LRU eviction) |
| **Error Sanitization** | Internal error details (SQL text, file paths) never exposed to API clients |
| **Cache Protection** | Cache invalidation restricted to localhost |
| **Error Boundary** | Frontend catches React render errors with recovery UI instead of blank screen |
| **Graceful Shutdown** | Caches cleared on SIGTERM via FastAPI lifespan handler |
| **Bounded Dependencies** | Python packages pinned to major version ranges (`>=X,<Y`) |

---

## Caching & Concurrency

### Two-Layer Cache
1. **In-memory TTL** (default 8h) — instant response for repeated requests
2. **Request coalescing** — if N threads request the same uncached key, only 1 query fires; N-1 wait on a `threading.Event`

### Thundering Herd Protection
When 4,000 users click "Generate Lineage" simultaneously on a cold cache:

| Without Protection | With Coalescing (implemented) |
|---|---|
| 4,000 duplicate DBSQL queries | 1 DBSQL query |
| Warehouse overwhelmed | 3,999 threads wait on Event |
| ~10-18s x 4,000 | ~10-18s total (leader) + <1ms (followers) |

If the leader thread fails, waiters use **jittered backoff** (0-10s spread + random 50-500ms delay) to avoid stampeding.

---

## Lineage Inference Engine

When `system.access.table_lineage` is not accessible, lineage is inferred:

1. **View definitions** — Parses `FROM`/`JOIN` references from `information_schema.views`
2. **Query history** — Scans recent CTAS/INSERT from `system.query.history` (configurable via `QUERY_HISTORY_DAYS`)
3. **Naming conventions** — Maps `raw_X` -> `cleaned_X` (medallion architecture)
4. **Column overlap** — Tables sharing >30% columns or 2+ matches are linked

All strategies are additive and deduplicated.

---

## Project Structure

```
lineage-explorer/
├── databricks.yml              # DABs config — THE canonical deployment config
├── app.yaml                    # Fallback for manual deployment only
├── requirements.txt            # Python deps (pinned ranges)
├── .gitignore
├── .databricksignore           # Excludes dev files from app deployment
├── backend/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app, middleware, validation, health check
│   ├── lineage_service.py      # DBSQL queries, cache, inference engine
│   └── models.py               # Pydantic models
└── frontend/
    ├── package.json
    ├── vite.config.ts           # Dev server proxy + build config
    ├── tsconfig.json
    ├── tailwind.config.ts
    ├── index.html
    ├── dist/                    # Built frontend (committed for deployment)
    └── src/
        ├── main.tsx             # Entry with ErrorBoundary
        ├── App.tsx              # Root: toolbar + canvas
        ├── api/client.ts        # Typed API client
        ├── store/lineageStore.ts
        ├── lib/elkLayout.ts     # ELK.js layout
        └── components/
            ├── graph/           # LineageCanvas, TableNode, AnimatedEdge
            ├── layout/          # Toolbar
            └── ui/              # Skeleton, SearchDialog, TableTooltip, ErrorBoundary
```

---

## Cost Comparison (Deployment Options)

Three deployment options exist. This repo is **Option 1 (Direct)**.

| Dimension | Option 1: Direct (this repo) | Option 2: Delta Cache | Option 3: Lakebase |
|-----------|-----------------------------|-----------------------|-------------------|
| **Warm cache latency** | <1ms | <1ms | <1ms |
| **Cold cache latency** | 7-18s (DBSQL) | 200-800ms (Delta) | 30ms (PostgreSQL) |
| **Cache survives restart** | No | Yes | Yes |
| **Monthly cost** | ~$55-65 | ~$60-77 | ~$95-117 |
| **Best for** | Demos, low traffic | Medium traffic | Production, 4000+ users |
| **Warehouse cost per user click** | $0 (served from cache) | $0 (served from cache) | $0 (served from cache) |
| **Cost scales with users?** | No — fixed app compute | No — fixed app compute | No — fixed app compute |

---

## Cost Optimization & Sustainable Economics

Lineage Explorer follows the **"Snapshot + Cached App" pattern** — the same architecture recommended for building Databricks Apps with import-mode-like economics. User clicks are effectively free from a warehouse-cost perspective.

### How It Works

| Phase | What Happens | Cost Impact |
|-------|-------------|-------------|
| **Cold cache miss** | One DBSQL query fires against `information_schema` + `system.access` | Single warehouse query (lightweight metadata, not raw data) |
| **Warm cache hit** | Response served from in-memory TTL cache (default 8h) | $0 — no warehouse query |
| **Concurrent requests** | Request coalescing: 4,000 users = 1 query, 3,999 wait <1ms | $0 additional — same single query |
| **App restart (Lakebase)** | Cache served from PostgreSQL (30ms cold read) | $0 — warehouse stays off |

The warehouse is **never hit per-click or per-user**. Cost is dominated by scheduled/periodic cache refreshes — exactly like Power BI import mode.

### Controlling App & Warehouse Uptime

Databricks Apps are billed **per hour of app compute** while running (Medium/Large size, fixed DBUs/hour). To minimize cost:

**App runtime window:**
- Run the app only during business hours (e.g., 08:00–18:00 M–F)
- Stop the app outside hours to save ~14 hours/day of compute
- With Lakebase, the persisted cache is available immediately on restart — no cold-start penalty

**Warehouse window:**
- Restrict the SQL warehouse to refresh/snapshot windows only
- Outside these periods, keep the warehouse in auto-stop mode
- The app continues serving from cache even when the warehouse is stopped

**Example schedule:**
```
App:       08:00–18:00 M–F (50 hours/week)
Warehouse: Auto-stop after 10 min idle (only wakes for cache refresh)
Result:    ~$43-55/mo app compute + ~$12-22/mo warehouse = ~$55-77/mo total
```

### Budget Policies & Monitoring

Track and cap Lineage Explorer costs using Databricks governance tools:

**Track usage via system billing tables:**
```sql
-- App compute cost (APPS SKU)
SELECT usage_date, SUM(usage_quantity) as app_dbus
FROM system.billing.usage
WHERE sku_name LIKE '%APPS%'
  AND usage_metadata.app_name = 'lineage-explorer'
GROUP BY usage_date;

-- Warehouse cost (SQL SKU)
SELECT usage_date, SUM(usage_quantity) as sql_dbus
FROM system.billing.usage
WHERE sku_name LIKE '%SQL%'
  AND usage_metadata.warehouse_id = '<warehouse-id>'
GROUP BY usage_date;
```

**Set guardrails:**
- **Serverless budget policies** — Set spending caps on APPS and SQL SKUs
- **Billing alerts** — Get notified if daily spend exceeds thresholds
- **Limited SPN permissions** — App service principal only gets `USE CATALOG` + `BROWSE` + `USE SCHEMA` (no broad `SELECT` on raw data)
- **Rate limiting** — 60 req/min/IP prevents runaway query patterns
- **No `SELECT *` on raw tables** — All app queries target lightweight metadata tables only

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| "No SQL warehouse available" | `DATABRICKS_WAREHOUSE_ID` not set | Pass `--var warehouse_id=<id>` during `bundle deploy` |
| Empty lineage graph | App SPN lacks BROWSE on catalog | Grant `BROWSE` on the target catalog to the app SPN |
| Catalog not visible in app | App SPN lacks USE CATALOG | Grant `USE CATALOG` on the target catalog to the app SPN |
| Only naming-convention edges | `system.access` not accessible | Grant `SELECT` on `system.access` to the app SPN |
| Query history timeout on cold start | Serverless warehouse cold + large history | Normal — app falls back to other inference methods. Lineage coverage improves on subsequent requests |
| `bundle deploy` host mismatch | Wrong profile or missing `--profile` flag | Use `--profile <name>` matching your `~/.databrickscfg` |
| `bundle deploy` missing variable | No `--var warehouse_id` provided | Add `--var warehouse_id=<id>` to the deploy command |
| Edges misaligned after expand | React Flow handle cache stale | Fixed — `useUpdateNodeInternals` called after animation |
| Blank white screen | Unhandled React error | Fixed — ErrorBoundary catches and shows recovery UI |
| 429 Too Many Requests | Rate limit exceeded | Wait and retry, or increase via `RATE_LIMIT_MAX_REQUESTS` env var |
| 400 "Invalid catalog" | Special chars in identifier | Use only alphanumeric + underscores |

---

## License

MIT
