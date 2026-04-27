import { defineConfig, devices } from "@playwright/test";

// Target URL is the deployed app — set APP_URL to override.
const APP_URL =
  process.env.APP_URL ||
  "https://lineage-explorer-direct-7474657661772683.aws.databricksapps.com";

// Databricks Apps sit behind an OAuth proxy. For external HTTP clients
// (Playwright, curl, SDKs), a Bearer token in the Authorization header is
// accepted for BOTH API routes and HTML/static asset fetches — verified
// against the dev app 2026-04-24.
//
// Mint the token via: scripts/mint-smoke-token.sh <profile>
// (SPN OAuth M2M client_credentials grant, scope=all-apis).
//
// Note on browser vs API tests: page.goto(...) works with the
// extraHTTPHeaders below because the proxy honors the Authorization
// header on the initial HTML request. If it ever stops working and
// the proxy starts requiring cookies, swap to `context.addCookies`
// with the `.auth` flow from docs.databricks.com/aws/en/dev-tools/databricks-apps/auth.
const TOKEN = process.env.SMOKE_TOKEN || "";

export default defineConfig({
  testDir: "./specs",
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,   // app is single-tenant; avoid stressing it from the test side
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: [["list"], ["html", { open: "never", outputFolder: "playwright-report" }]],
  use: {
    baseURL: APP_URL,
    extraHTTPHeaders: TOKEN
      ? { Authorization: `Bearer ${TOKEN}` }
      : undefined,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
