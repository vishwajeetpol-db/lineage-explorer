import { test, expect } from "@playwright/test";

// Smoke test 2: backend API is healthy and returns expected shape.
// What this catches: warehouse mis-config, SQL permission regressions, cache
// corruption, Pydantic schema drift. This is the cheapest single test that
// would have caught the blank-canvas bug before the customer demo — if
// /api/lineage returns nodes but the UI shows nothing, the UI is at fault.
test("API health + lineage endpoint returns a parseable response", async ({ request }) => {
  const health = await request.get("/health");
  expect(health.status(), "health endpoint").toBe(200);
  expect(await health.json()).toMatchObject({ status: "ok" });

  // Use the demo schema seeded by setup_demo_lineage.py.
  const lineage = await request.get("/api/lineage", {
    params: { catalog: "ws_us_e2_vish_aws_catalog", schema: "lineage_demo" },
  });
  expect(lineage.status(), "lineage endpoint").toBe(200);

  const body = await lineage.json();
  expect(body).toHaveProperty("nodes");
  expect(body).toHaveProperty("edges");
  expect(Array.isArray(body.nodes)).toBe(true);
  expect(Array.isArray(body.edges)).toBe(true);
  expect(body.nodes.length, "demo schema should return nodes").toBeGreaterThan(0);
});
