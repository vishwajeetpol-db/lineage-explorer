# Smoke tests

Tiny Playwright suite run against a **deployed** app URL (staging or prod) before every customer-facing release.

Purpose: catch the handful of failure modes that matter — app won't boot, API doesn't return data, cache-hit leaves a blank canvas — without trying to be a full test suite.

## One-time setup

```bash
cd tests/smoke
npm install
npm run install:browsers   # downloads Chromium (~150MB)
```

## Run against the deployed app

```bash
# Default APP_URL points to lineage-explorer-direct. Override if needed.
npm test
```

Databricks Apps require a Bearer token (the platform proxy rejects anonymous requests). For a U2M (user) profile, mint a token directly:

```bash
export SMOKE_TOKEN=$(databricks auth token --profile fe-vm-vish-aws | jq -r .access_token)
npm test
```

For an SPN/M2M profile, use an OAuth `client_credentials` grant with `scope=all-apis` against `$HOST/oidc/v1/token`. Token lasts 1 hour.

## Run against a custom URL

```bash
APP_URL=https://<your-app>.aws.databricksapps.com npm test
```

## What the three tests catch

| Test | Guards against |
|---|---|
| `landing.spec.ts` | Bundle build failure, missing static assets, uvicorn crash on boot, auth redirect loop |
| `api-health.spec.ts` | Warehouse mis-config, UC permission regression, Pydantic schema drift, empty response bugs |
| `cache-hit-render.spec.ts` | **The blank-canvas regression from 2026-04-23** — cache hit + re-render stale-state race |

## Adding a test

New test = new bug + its regression guard. Don't add tests just for coverage; add them when a real failure mode needs locking down.
