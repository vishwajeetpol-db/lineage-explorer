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

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Databricks App                            │
│                                                             │
│  ┌──────────────┐        ┌──────────────────────────────┐   │
│  │   FastAPI     │        │       React Frontend         │   │
│  │   Backend     │        │                              │   │
│  │              │───────>│  React Flow (DAG canvas)     │   │
│  │  /api/       │        │  ELK.js (layered layout)     │   │
│  │  /health     │        │  Framer Motion (animations)  │   │
│  │              │        │  Zustand (state management)  │   │
│  │  Middleware:  │        │  Tailwind CSS (dark theme)   │   │
│  │  • Rate limit │        │  ErrorBoundary (crash guard) │   │
│  │  • Validation │        │                              │   │
│  │  • Sanitize   │        │  Client-side column lineage  │   │
│  └──────┬───────┘        └──────────────────────────────┘   │
│         │                                                    │
│         │  Databricks SDK (unified auth)                     │
│         ▼                                                    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Unity Catalog                                        │    │
│  │  • information_schema.tables / columns / views        │    │
│  │  • system.access.table_lineage / column_lineage       │    │
│  │  • system.query.history                               │    │
│  └──────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Authentication Model

The app uses the **Databricks SDK unified authentication**, which auto-detects credentials:

| Method | Env Vars Required | Use Case |
|--------|------------------|----------|
| **Databricks App (auto)** | None — injected automatically | Production: running as a Databricks App |
| **Service Principal (OAuth M2M)** | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | CI/CD, automation, standalone deployment |
| **Personal Access Token** | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | Local development only |

**When deployed as a Databricks App**, the app's built-in service principal is used automatically — no credentials need to be configured.

---

## Prerequisites

Before deploying to any Databricks workspace, you need:

### 1. Databricks Workspace
- Unity Catalog enabled
- A SQL Warehouse (serverless or pro) — note the warehouse ID

### 2. Node.js & Python
- Node.js 18+ with npm (for building frontend)
- Python 3.10+ (for backend)

### 3. Databricks CLI
- Version 0.200+ (`databricks --version`)
- Authenticated to your workspace (`databricks auth login`)

---

## Quick Start: Deploy to Any Workspace

### Step 1: Clone and Build

```bash
git clone <repo-url>
cd lineage-explorer

# Build frontend
cd frontend && npm install && npm run build && cd ..
```

### Step 2: Configure app.yaml

Edit `app.yaml` — only one value to change:

```yaml
command:
  - uvicorn
  - backend.main:app
  - --host
  - 0.0.0.0
  - --port
  - 8000
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "<your-warehouse-id>"    # <-- Replace this
```

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATABRICKS_WAREHOUSE_ID` | (auto-detect) | SQL warehouse ID for queries |
| `CACHE_TTL_SECONDS` | `28800` (8h) | How long cached lineage data lives |

### Step 3: Create the App

```bash
databricks apps create lineage-explorer \
  --description "Unity Catalog Lineage Explorer"
```

### Step 4: Grant SPN Permissions

Get the app's auto-generated SPN client ID:
```bash
databricks apps get lineage-explorer -o json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['service_principal_client_id'])"
```

Grant **minimum privileges** (no SELECT needed):
```sql
-- Replace <catalog>, <schema>, and <app-spn-client-id>
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-spn-client-id>`;
GRANT BROWSE ON CATALOG <catalog> TO `<app-spn-client-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<app-spn-client-id>`;
```

Grant warehouse access via UI: **SQL Warehouses > Your Warehouse > Permissions > Add SPN with "Can Use"**

### Step 5: Sync and Deploy

```bash
# Use databricks sync (respects .databricksignore — won't upload node_modules, .venv, etc.)
databricks sync . /Workspace/Users/<your-email>/lineage-explorer --watch=false

# Deploy
databricks apps deploy lineage-explorer
```

### Step 6: Verify

Open the app URL from `databricks apps get lineage-explorer`. Select a catalog and schema, click "Generate Lineage".

Health check: `curl https://<app-url>/health` should return `{"status":"ok","version":"1.2.0"}`

---

## Optional: Enhanced Lineage Privileges

Without optional privileges, the app infers lineage from naming conventions and column overlap (~60-70% coverage). Adding these improves coverage:

```sql
-- Enables view definition parsing (~85-90% coverage)
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<app-spn-client-id>`;

-- Enables real system lineage (~100% coverage, needs metastore admin)
GRANT USE CATALOG ON CATALOG system TO `<app-spn-client-id>`;
GRANT USE SCHEMA ON SCHEMA system.access TO `<app-spn-client-id>`;
GRANT SELECT ON SCHEMA system.access TO `<app-spn-client-id>`;
GRANT USE SCHEMA ON SCHEMA system.query TO `<app-spn-client-id>`;
GRANT SELECT ON SCHEMA system.query TO `<app-spn-client-id>`;
```

| Privileges Granted | Lineage Strategies | Approx Coverage |
|---|---|---|
| Minimum only (no SELECT) | Naming conventions + column overlap | ~60-70% |
| + SELECT on schema | Above + view definition parsing | ~85-90% |
| + SELECT on system tables | Real Unity Catalog lineage | ~100% |

The app **never crashes** regardless of privilege level — all optional queries are wrapped in try/except with graceful fallback.

---

## Local Development

```bash
# Terminal 1 — Backend
pip install -r requirements.txt
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-pat>"   # or use SP OAuth
export DATABRICKS_WAREHOUSE_ID="<warehouse-id>"
uvicorn backend.main:app --reload --port 8000

# Terminal 2 — Frontend (with hot reload + API proxy to :8000)
cd frontend && npm run dev
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
| **Rate Limiting** | 60 req/min/IP on API endpoints, bounded memory (max 10K tracked IPs with LRU eviction) |
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
| ~10-18s × 4,000 | ~10-18s total (leader) + <1ms (followers) |

If the leader thread fails, waiters use **jittered backoff** (0-10s spread + random 50-500ms delay) to avoid stampeding.

---

## Lineage Inference Engine

When `system.access.table_lineage` is not accessible, lineage is inferred:

1. **View definitions** — Parses `FROM`/`JOIN` references from `information_schema.views`
2. **Query history** — Scans recent CTAS/INSERT from `system.query.history`
3. **Naming conventions** — Maps `raw_X` -> `cleaned_X` (medallion architecture)
4. **Column overlap** — Tables sharing >30% columns or 2+ matches are linked

All strategies are additive and deduplicated.

---

## Project Structure

```
lineage-explorer/
├── app.yaml                    # Databricks App config (command + env vars)
├── requirements.txt            # Python deps (pinned ranges)
├── .gitignore
├── .databricksignore           # Excludes node_modules, .venv, etc from sync
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

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| "No SQL warehouse available" | `DATABRICKS_WAREHOUSE_ID` not set | Set it in `app.yaml` |
| Empty lineage graph | SPN lacks BROWSE on catalog | Grant `BROWSE` on the target catalog |
| Only naming-convention edges | `system.access` not accessible | Grant `SELECT` on `system.access` |
| Edges misaligned after expand | React Flow handle cache stale | Fixed — `useUpdateNodeInternals` called after animation |
| Blank white screen | Unhandled React error | Fixed — ErrorBoundary catches and shows recovery UI |
| 429 Too Many Requests | Rate limit (60 req/min/IP) | Wait and retry |
| 400 "Invalid catalog" | Special chars in identifier | Use only alphanumeric + underscores |

---

## License

MIT
