# Load tests

Simple concurrent load test using stdlib only.

## Run

```bash
export APP_URL=https://lineage-explorer-direct-7474657661772683.aws.databricksapps.com
export SMOKE_TOKEN=$(databricks auth token --profile fe-vm-vish-aws | jq -r .access_token)

# Moderate load
python tests/load/load_test.py --users 20 --requests-per-user 5

# Heavy load (100 users × 10 requests = 1000 total)
python tests/load/load_test.py --users 100 --requests-per-user 10
```

## What the numbers should look like

| Metric | Healthy | Warning | Critical |
|---|---|---|---|
| Success rate | 100% | 95-99% | <95% |
| p50 latency | <500ms | 500-2000ms | >2000ms |
| p95 latency | <2s | 2-10s | >10s |
| p99 latency | <5s | 5-20s | >20s |

**Note on first-run behavior**: the first N requests per schema are slow because the cache is cold — DBSQL roundtrip + graph construction can take 5-15s. Once the cache is warm, everything should be sub-second.

## When to run

- Before a customer demo if you've changed caching, routing, or concurrency code
- After scaling the SQL warehouse up or down
- When investigating user-reported slowness
- As a quarterly regression check

This is a **manual** tool, not part of CI — sustained load against a live warehouse costs money.
