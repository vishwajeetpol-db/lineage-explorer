# Lineage Explorer

**Interactive DAG visualization for Unity Catalog lineage** — end-to-end table & column lineage, pipeline/job visibility, serverless cost, and Delta Sharing, across every catalog in your metastore. One command to deploy; zero access to your row data.

![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white) ![React](https://img.shields.io/badge/React-18-blue) ![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?style=flat&logo=typescript&logoColor=white) ![Databricks](https://img.shields.io/badge/Databricks_Apps-FF3621?style=flat&logo=databricks&logoColor=white) ![ELK.js](https://img.shields.io/badge/ELK.js-layout-orange)

Unity Catalog captures lineage from every SQL operation — but reading it means querying system tables by hand. Lineage Explorer turns those system tables into an interactive graph, deployed as a Databricks App and shared across your whole workspace.

## Features

- **Select anything → full end-to-end lineage.** Pick any table (search or browse) and it auto-traces the complete lineage cone — every upstream source back to every downstream target — **across all catalogs and schemas**, with the mediating pipeline/job nodes. No buttons to press; the only control is the view mode.
- **Column-level lineage** traced from real `system.access.column_lineage` edges — no name-matching heuristics, zero false positives.
- **Delta Sharing, always in the picture.** Shared-in sources and shared-out targets show up as part of lineage (with provider/recipient boundary nodes). The trace stops honestly at the metastore boundary — we can't read the other account.
- **Serverless cost on pipeline/job nodes** — 30-day list price from `system.billing`, with a client-side discount control.
- **Three view modes** — Tables, Pipelines, or Full.
- **Scales to thousands of users on one query** — request coalescing + a memory-bounded LRU/TTL cache mean the warehouse is barely touched.
- **Admin live mode + built-in ops dashboard** (P50/P95/P99 latency, memory, cache inventory).
- **Deep-link embeddable** — `?table=catalog.schema.table` from any dashboard or tool.
- **Metadata-only access** — the app reads `BROWSE` + system tables, never your table data.

## Quick start

**Prerequisites:** Databricks CLI v0.239+, a SQL warehouse, Unity Catalog, and **system tables enabled** — `system.access` (lineage) and `system.billing` (cost). *Without `system.access`, the app shows no lineage — the #1 "empty app" cause.*

```bash
databricks auth login --profile <profile>
databricks bundle deploy -t dev --profile <profile> --var warehouse_id=<warehouse-id>
databricks bundle run lineage-explorer -t dev --profile <profile>
```

Then, as a **metastore admin**, grant the app's service principal read on the system tables + `BROWSE` on the catalogs you want explorable — fill in the placeholders in **[`setup.sql`](setup.sql)** and run it. (End-to-end traces span catalogs, so grant on every catalog in the lineage you want visible.)

## Documentation

The full reference lives in **[docs/REFERENCE.md](docs/REFERENCE.md)**:

| Topic | |
|---|---|
| [Deploy & permissions](docs/REFERENCE.md#permissions-reference) | Prerequisites, SPN grants, DABs deploy, post-deploy setup |
| [Features & view modes](docs/REFERENCE.md#features) | Graph, canvas, node/edge types, depth control |
| [User identity & live mode](docs/REFERENCE.md#user-identity--live-mode) | On-behalf-of auth, admin gating |
| [Caching & concurrency](docs/REFERENCE.md#caching--concurrency) | Cache keys, single-flight, memory sizing |
| [Cost optimization](docs/REFERENCE.md#cost-optimization) | Warehouse sizing, cost-cache behavior, billing lag |
| [Architecture & scaling](docs/REFERENCE.md#architecture) | Single-process model, scaling beyond one process |
| [Configuration & API](docs/REFERENCE.md#configuration) | Env vars, endpoints |
| [Troubleshooting](docs/REFERENCE.md#troubleshooting) | Common symptoms & fixes |

## Tech stack

FastAPI + Uvicorn (single process, 64-thread pool) · Databricks SDK + DBSQL over UC system tables · React + TypeScript + React Flow + ELK.js (layout in a Web Worker) · deployed via Databricks Asset Bundles.

## License

See [License](docs/REFERENCE.md#license).
