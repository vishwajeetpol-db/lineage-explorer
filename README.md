# Unity Catalog Lineage Explorer

An interactive DAG visualization app that renders table and column-level dependency graphs for any Databricks Unity Catalog schema. Select a catalog and schema, and instantly see how tables, views, and materialized views connect — from raw ingestion through silver transformations to gold analytics layers.

---

## Why This Exists

Understanding data lineage is critical for data governance, impact analysis, and debugging pipelines. Unity Catalog provides system tables for lineage, but they can take 24+ hours to populate and lack a visual interface. This app:

- **Visualizes lineage instantly** — even when system tables are empty, using multi-strategy inference (view definition parsing, query history, naming conventions, column overlap heuristics)
- **Supports column-level lineage** — toggle column lineage mode, expand a table, click a column, and see exactly which upstream/downstream tables share that column
- **Runs as a Databricks App** — deployed via Databricks Asset Bundles (DABs), with built-in OAuth SSO authentication, no separate infrastructure needed
- **Handles large schemas** — tested with 150+ tables across 6 business domains with cross-dependencies

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Databricks App                        │
│                                                         │
│  ┌──────────────┐     ┌──────────────────────────────┐  │
│  │   FastAPI     │     │       React Frontend         │  │
│  │   Backend     │     │                              │  │
│  │              │────▶│  React Flow (DAG canvas)     │  │
│  │  /api/       │     │  ELK.js (layered layout)     │  │
│  │  catalogs    │     │  Framer Motion (animations)  │  │
│  │  schemas     │     │  Zustand (state management)  │  │
│  │  lineage     │     │  Tailwind CSS (dark theme)   │  │
│  │  column-     │     │                              │  │
│  │  lineage     │     │  Client-side column lineage  │  │
│  └──────┬───────┘     └──────────────────────────────┘  │
│         │                                               │
│         │  Databricks SDK                               │
│         │  (SQL Statement Execution API)                │
│         ▼                                               │
│  ┌──────────────────────────────────┐                   │
│  │  Unity Catalog                   │                   │
│  │  • information_schema.tables     │                   │
│  │  • information_schema.columns    │                   │
│  │  • information_schema.views      │                   │
│  │  • system.access.table_lineage   │                   │
│  │  • system.access.column_lineage  │                   │
│  │  • system.query.history          │                   │
│  └──────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────┘
```

### Lineage Inference Strategy

When `system.access.table_lineage` is empty (common in new workspaces), the backend infers lineage using four strategies in order:

1. **View definition parsing** — Extracts `FROM`/`JOIN` references from `information_schema.views`
2. **Query history analysis** — Parses `CREATE TABLE AS SELECT` and `INSERT INTO ... SELECT` from `system.query.history`
3. **Naming convention matching** — Links `raw_*` tables to their `cleaned_*` counterparts
4. **Column overlap heuristic** — If a gold table shares 2+ columns (or >30% overlap) with a silver table, infers a dependency

### Column Lineage (Client-Side)

Column lineage is computed entirely in the browser for instant response (~<10ms). When you click a column, the frontend:
1. Finds all upstream/downstream tables from existing table-level edges
2. Checks if those tables have a column with the same name
3. Draws column-level edges between matching columns

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Graph Rendering** | [React Flow](https://reactflow.dev/) | Node-based DAG canvas with pan, zoom, minimap |
| **Graph Layout** | [ELK.js](https://github.com/kieler/elkjs) | Layered DAG layout (Eclipse Layout Kernel) — superior to Dagre for complex graphs |
| **Animations** | [Framer Motion](https://www.framer.com/motion/) | Smooth expand/collapse, highlight/dim, tooltip transitions |
| **State Management** | [Zustand](https://github.com/pmndrs/zustand) | Lightweight React state store |
| **Styling** | [Tailwind CSS](https://tailwindcss.com/) | Utility-first dark theme with custom design tokens |
| **Icons** | [Lucide React](https://lucide.dev/) | Consistent icon set |
| **UI Primitives** | [Radix UI](https://www.radix-ui.com/) | Accessible dropdown, toggle, dialog components |
| **Build Tool** | [Vite](https://vitejs.dev/) | Fast dev server and production bundler |
| **Backend** | [FastAPI](https://fastapi.tiangolo.com/) + [Uvicorn](https://www.uvicorn.org/) | Async Python API server |
| **Databricks SDK** | [databricks-sdk](https://docs.databricks.com/dev-tools/sdk-python.html) | SQL Statement Execution API for metadata queries |
| **Deployment** | [Databricks Apps](https://docs.databricks.com/dev-tools/databricks-apps/index.html) + [DABs](https://docs.databricks.com/dev-tools/bundles/index.html) | Hosted app with OAuth SSO, CI/CD pipeline |

---

## Project Structure

```
lineage-explorer/
├── app.yaml                    # Databricks App config (startup command, env vars)
├── databricks.yml              # DABs bundle config (dev/prod targets)
├── requirements.txt            # Python dependencies
├── generate_stress_test.py     # Script to create 150 test tables across 6 domains
├── .databricksignore           # Files to exclude from Databricks deployment
│
├── backend/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app — 4 API endpoints + static file serving
│   ├── models.py               # Pydantic models (TableNode, LineageEdge, etc.)
│   └── lineage_service.py      # Core service — SQL queries, lineage inference logic
│
└── frontend/
    ├── package.json            # Node.js dependencies
    ├── vite.config.ts          # Vite build config with API proxy
    ├── tailwind.config.ts      # Custom theme (colors, animations, shadows, fonts)
    ├── tsconfig.json           # TypeScript config
    ├── index.html              # Entry HTML with Google Fonts
    │
    ├── public/
    │   └── favicon.svg         # Custom gradient lineage icon
    │
    ├── src/
    │   ├── main.tsx            # React entry point
    │   ├── App.tsx             # Main app shell (toolbar + canvas)
    │   ├── api/client.ts       # TypeScript API client
    │   ├── store/lineageStore.ts   # Zustand state management
    │   ├── lib/elkLayout.ts    # ELK.js layout configuration
    │   ├── styles/globals.css  # Tailwind + React Flow style overrides
    │   │
    │   └── components/
    │       ├── graph/
    │       │   ├── LineageCanvas.tsx  # Main canvas — layout, highlight, column lineage
    │       │   ├── TableNode.tsx      # Custom node — compact/expanded modes, column rows
    │       │   └── AnimatedEdge.tsx   # Custom edge — glow, animated dashes, traveling dot
    │       ├── layout/
    │       │   └── Toolbar.tsx        # Catalog/schema dropdowns, column toggle, search
    │       └── ui/
    │           ├── TableTooltip.tsx   # Hover tooltip with metadata
    │           ├── SearchDialog.tsx   # Cmd+K search overlay
    │           └── Skeleton.tsx       # Loading skeleton
    │
    └── dist/                   # Production build output (served by FastAPI)
```

---

## Prerequisites

- **Databricks Workspace** with Unity Catalog enabled
- **SQL Warehouse** (serverless or pro) accessible from the app
- **Databricks CLI** v0.200+ (for deployment)
- **Node.js** 18+ and **npm** (for frontend build)
- **Python** 3.10+ (for backend)

---

## Setup & Deployment

### 1. Clone and Install

```bash
git clone <repo-url> && cd lineage-explorer

# Backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Frontend
cd frontend
npm install
npm run build
cd ..
```

### 2. Configure

Edit `databricks.yml`:
- Set `workspace.host` to your Databricks workspace URL
- Set `variables.warehouse_id.default` to your SQL warehouse ID

Edit `app.yaml`:
- Set `DATABRICKS_WAREHOUSE_ID` to your SQL warehouse ID

### 3. Authenticate

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com --profile my-workspace
```

### 4. Create the Databricks App

```bash
databricks apps create lineage-explorer \
  --description "Unity Catalog Lineage Explorer" \
  --profile my-workspace
```

### 5. Grant Permissions to the App Service Principal

After creating the app, Databricks generates a service principal (SPN) for it. Grant it access to your catalog:

```sql
-- Replace <spn-client-id> with the app's service_principal_client_id
GRANT USE CATALOG ON CATALOG your_catalog TO `<spn-client-id>`;
GRANT USE SCHEMA ON SCHEMA your_catalog.your_schema TO `<spn-client-id>`;
GRANT SELECT ON SCHEMA your_catalog.your_schema TO `<spn-client-id>`;

-- For system lineage tables (requires metastore admin):
GRANT USE CATALOG ON CATALOG system TO `<spn-client-id>`;
GRANT USE SCHEMA ON SCHEMA system.access TO `<spn-client-id>`;
GRANT SELECT ON SCHEMA system.access TO `<spn-client-id>`;
GRANT USE SCHEMA ON SCHEMA system.query TO `<spn-client-id>`;
GRANT SELECT ON SCHEMA system.query TO `<spn-client-id>`;
```

> **Note:** If you don't have metastore admin access to grant system catalog permissions, the app will automatically fall back to the inference-based lineage strategy.

### 6. Deploy

```bash
# Upload source code
databricks workspace import-dir . \
  /Workspace/Users/you@company.com/lineage-explorer \
  --overwrite --profile my-workspace

# Start the app (if not already running)
databricks apps start lineage-explorer --profile my-workspace

# Deploy
databricks apps deploy lineage-explorer \
  --source-code-path /Workspace/Users/you@company.com/lineage-explorer \
  --profile my-workspace
```

Or use DABs:
```bash
databricks bundle deploy --target dev --profile my-workspace
```

### 7. Access

The app URL will be shown after deployment. It follows the pattern:
```
https://lineage-explorer-<workspace-id>.aws.databricksapps.com
```

Login is handled automatically via Databricks OAuth SSO.

---

## Local Development

```bash
# Terminal 1 — Backend
source .venv/bin/activate
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapiXXXXXXXX
export DATABRICKS_WAREHOUSE_ID=your_warehouse_id
uvicorn backend.main:app --reload --port 8000

# Terminal 2 — Frontend (with hot reload + API proxy)
cd frontend
npm run dev    # Starts on http://localhost:5173, proxies /api to :8000
```

---

## Stress Testing

The included `generate_stress_test.py` creates 150 tables across 6 business domains (ecommerce, finance, HR, marketing, supply chain, support) with bronze/silver/gold layers and cross-domain dependencies.

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapiXXXXXXXX
export DATABRICKS_WAREHOUSE_ID=your_warehouse_id
export STRESS_TEST_CATALOG=your_catalog
export STRESS_TEST_SCHEMA=lineage_stress_test

# Create the schema first
# (run in Databricks SQL): CREATE SCHEMA your_catalog.lineage_stress_test;

python3 generate_stress_test.py
```

---

## Features

- **Interactive DAG** — Pan, zoom, minimap navigation across large schemas
- **Table metadata on hover** — Owner, type, column count, created/updated dates, upstream/downstream counts
- **Path highlighting** — Click or hover a table to highlight its full upstream + downstream lineage path
- **Column lineage mode** — Toggle on, expand a table, click a column to see column-level flow
- **Cmd+K search** — Quickly find and navigate to any table
- **Responsive layout** — ELK.js layered algorithm handles complex DAGs with minimal edge crossings
- **Animated edges** — Glowing indigo edges with animated dashes and traveling dots for highlighted paths
- **Type indicators** — Color-coded badges for TABLE (blue), VIEW (green), MATERIALIZED VIEW (amber)

---

## License

MIT
