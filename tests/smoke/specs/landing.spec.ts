import { test, expect } from "@playwright/test";

// Smoke test 1: the app comes up and serves the landing page.
// What this catches: bundle build failures, missing static assets, backend
// not starting (uvicorn errors show HTML 500), OAuth misconfiguration (HTML
// redirect-to-login loops).
test("landing page renders with Lineage Explorer branding", async ({ page }) => {
  const response = await page.goto("/", { waitUntil: "domcontentloaded" });
  expect(response?.status(), "landing should return 2xx").toBeLessThan(400);

  // The SPA shell injects "Lineage Explorer" into the DOM on boot.
  await expect(page.getByText("Lineage Explorer", { exact: false })).toBeVisible({
    timeout: 20_000,
  });
});
