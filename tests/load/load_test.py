"""
Simple load test — pure stdlib, no external deps.

Simulates N concurrent users hitting /api/lineage against a deployed app URL
and reports latency percentiles + error rate. Use before customer demos to
verify the app can handle expected traffic.

Usage:
  export APP_URL=https://lineage-explorer-direct-7474657661772683.aws.databricksapps.com
  export SMOKE_TOKEN=$(databricks auth token --profile fe-vm-vish-aws | jq -r .access_token)
  python tests/load/load_test.py --users 50 --requests-per-user 5

What to expect on the warehouse:
  - First 50 requests: slow (p95 ~5-15s) as schema lineage populates cache
  - Steady state: p95 should be <1s (cache hits)
  - Error rate: should be 0%
"""
import argparse
import os
import statistics
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass


@dataclass
class Result:
    success: bool
    latency_ms: float
    status: int = 0
    error: str = ""


def make_request(url: str, token: str, timeout: int = 60) -> Result:
    start = time.time()
    try:
        req = urllib.request.Request(url)
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()   # drain body so we measure full roundtrip
            elapsed = (time.time() - start) * 1000
            return Result(success=resp.status == 200, latency_ms=elapsed, status=resp.status)
    except urllib.error.HTTPError as e:
        elapsed = (time.time() - start) * 1000
        return Result(False, elapsed, e.code, f"HTTP {e.code}")
    except Exception as e:
        elapsed = (time.time() - start) * 1000
        return Result(False, elapsed, 0, f"{type(e).__name__}: {e}")


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = min(int(len(sorted_data) * p / 100), len(sorted_data) - 1)
    return sorted_data[idx]


def run(app_url: str, token: str, users: int, requests_per_user: int, catalog: str, schema: str):
    endpoint = f"{app_url.rstrip('/')}/api/lineage?catalog={catalog}&schema={schema}"
    total_requests = users * requests_per_user
    print(f"Target: {endpoint}")
    print(f"Load: {users} concurrent users × {requests_per_user} requests each = {total_requests} total")
    print()

    start = time.time()
    results: list[Result] = []
    with ThreadPoolExecutor(max_workers=users) as pool:
        futures = [pool.submit(make_request, endpoint, token) for _ in range(total_requests)]
        for i, fut in enumerate(futures, 1):
            r = fut.result()
            results.append(r)
            if i % max(1, total_requests // 10) == 0:
                ok = sum(1 for x in results if x.success)
                print(f"  [{i}/{total_requests}] ok={ok} fail={i-ok}")

    total_time = time.time() - start

    successes = [r for r in results if r.success]
    failures = [r for r in results if not r.success]
    latencies = [r.latency_ms for r in successes]

    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Total:         {len(results)}")
    print(f"Success:       {len(successes)} ({len(successes)/len(results)*100:.1f}%)")
    print(f"Failure:       {len(failures)} ({len(failures)/len(results)*100:.1f}%)")
    print(f"Elapsed:       {total_time:.1f}s")
    print(f"Throughput:    {len(results)/total_time:.1f} req/s")
    print()
    if latencies:
        print("Latency (successful requests, ms):")
        print(f"  min:   {min(latencies):.0f}")
        print(f"  p50:   {percentile(latencies, 50):.0f}")
        print(f"  p95:   {percentile(latencies, 95):.0f}")
        print(f"  p99:   {percentile(latencies, 99):.0f}")
        print(f"  max:   {max(latencies):.0f}")
        print(f"  mean:  {statistics.mean(latencies):.0f}")

    if failures:
        print()
        print("Sample failures:")
        for r in failures[:5]:
            print(f"  {r.status} — {r.error}")

    # Exit nonzero if there were any failures — makes this usable in CI
    return 0 if not failures else 1


# Safety caps — load tests cost money because each request hits a real
# DBSQL warehouse. If you legitimately need to exceed these, use the
# --i-accept-cost flag and explain in the commit message.
DEFAULT_USER_CAP = 50
DEFAULT_REQUESTS_CAP = 20


def main():
    p = argparse.ArgumentParser(
        description="Load test against a DEPLOYED Lineage Explorer app. "
                    "Uses real DBSQL cycles — don't point at prod without --prod."
    )
    p.add_argument("--url", default=os.environ.get("APP_URL", ""))
    p.add_argument("--token", default=os.environ.get("SMOKE_TOKEN", ""))
    p.add_argument("--users", type=int, default=20)
    p.add_argument("--requests-per-user", type=int, default=5)
    p.add_argument("--catalog", default="ws_us_e2_vish_aws_catalog")
    p.add_argument("--schema", default="lineage_demo")
    p.add_argument(
        "--i-accept-cost",
        action="store_true",
        help=f"Required to exceed --users {DEFAULT_USER_CAP} or --requests-per-user {DEFAULT_REQUESTS_CAP}",
    )
    p.add_argument(
        "--prod",
        action="store_true",
        help="Required to run against a URL that looks like prod (lineage-explorer.*, no -dev/-staging)",
    )
    args = p.parse_args()

    if not args.url:
        print("ERROR: --url or APP_URL env var required", file=sys.stderr)
        return 2

    # Cost safety cap
    if (args.users > DEFAULT_USER_CAP or args.requests_per_user > DEFAULT_REQUESTS_CAP) and not args.i_accept_cost:
        print(
            f"ERROR: requested load exceeds safety cap "
            f"(users={args.users}>{DEFAULT_USER_CAP} or requests-per-user={args.requests_per_user}>{DEFAULT_REQUESTS_CAP}).\n"
            f"       Pass --i-accept-cost to override. Each request bills DBSQL.",
            file=sys.stderr,
        )
        return 2

    # Prod-pointing guard
    url_lower = args.url.lower()
    looks_prod = (
        "lineage-explorer" in url_lower
        and "-dev" not in url_lower
        and "-staging" not in url_lower
        and "localhost" not in url_lower
    )
    if looks_prod and not args.prod:
        print(
            f"ERROR: {args.url} looks like a prod URL. Refusing to load-test prod without --prod.\n"
            f"       Use the staging app instead, or pass --prod if you really mean it.",
            file=sys.stderr,
        )
        return 2

    return run(args.url, args.token, args.users, args.requests_per_user, args.catalog, args.schema)


if __name__ == "__main__":
    sys.exit(main())
