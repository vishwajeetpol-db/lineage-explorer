import { test, expect } from "@playwright/test";

// Smoke test 3: THE bug from the demo.
// Load a table via deep-link, wait for render, then navigate to a different
// table in the same schema (which will be a cache hit). The graph must render.
//
// Why this test exists: on 2026-04-23 the app showed "cache <1ms" in the
// header but a blank canvas — the layout promise from the first navigation
// resolved after the second, leaving stale state. The fix (AbortSignal in
// layoutGraph, staging deploys before prod) is regression-guarded by this test.
test("cache-hit navigation still renders the graph", async ({ page }) => {
  // First load — warms the backend cache
  await page.goto(
    "/?table=ws_us_e2_vish_aws_catalog.lineage_demo.customer_orders",
    { waitUntil: "domcontentloaded" }
  );
  await expect(
    page.locator('[class*="react-flow"]').first(),
    "react-flow canvas should mount on first load"
  ).toBeVisible({ timeout: 30_000 });

  // React Flow renders nodes as .react-flow__node — we expect at least one
  await expect(
    page.locator(".react-flow__node").first(),
    "at least one node should be visible after first load"
  ).toBeVisible({ timeout: 20_000 });

  // Second load — deep-link a DIFFERENT table in the same schema → cache hit
  await page.goto(
    "/?table=ws_us_e2_vish_aws_catalog.lineage_demo.executive_summary",
    { waitUntil: "domcontentloaded" }
  );

  // Canvas must re-render (not stay blank from cached stale layout)
  await expect(
    page.locator(".react-flow__node").first(),
    "nodes must render on cache-hit navigation (regression test for blank-canvas bug)"
  ).toBeVisible({ timeout: 20_000 });

  // Cache chip should be visible — confirms we actually hit the cache path
  await expect(page.getByText(/cache\s*</i)).toBeVisible({ timeout: 10_000 });
});
