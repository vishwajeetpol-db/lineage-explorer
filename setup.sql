-- =============================================================================
-- Lineage Explorer — one-time grants for the app's service principal.
--
-- The app runs as its own service principal (SP) and reads ONLY metadata +
-- system tables — never your row data. After `databricks bundle deploy`, a
-- METASTORE ADMIN runs this once so the deployed app can actually see lineage,
-- cost, and sharing.
--
-- Replace the two placeholders below, then run in a SQL editor / DBSQL:
--   :APP_SP   the app's service principal Application ID
--             (Apps UI → your app → "App resources"/OAuth, or
--              `databricks apps get <app-name>` → service_principal_client_id)
--   :CATALOG  a catalog you want explorable — repeat the CATALOG block per catalog
--
-- Least privilege: BROWSE exposes names/metadata for lineage, NOT table data.
-- Grant SELECT on a catalog only if you also want the in-app data preview to read rows.
-- =============================================================================

-- 1) System tables — lineage, cost, and sharing metadata (read-only) ----------
GRANT USE CATALOG ON CATALOG system                       TO `:APP_SP`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.access          TO `:APP_SP`;  -- table_lineage, column_lineage, audit
GRANT USE SCHEMA, SELECT ON SCHEMA system.billing         TO `:APP_SP`;  -- usage, list_prices (serverless cost)
GRANT USE SCHEMA, SELECT ON SCHEMA system.information_schema TO `:APP_SP`;  -- shares / recipients / providers / *_share_usage

-- 2) Per-catalog metadata browse (repeat for each explorable catalog) ---------
GRANT USE CATALOG ON CATALOG `:CATALOG`                    TO `:APP_SP`;
GRANT BROWSE      ON CATALOG `:CATALOG`                    TO `:APP_SP`;
-- Optional — only if you want the app to read sample rows / the data preview:
-- GRANT SELECT    ON CATALOG `:CATALOG`                    TO `:APP_SP`;

-- 3) (Optional) Live mode — members of this group can bypass cache.
--    Set ADMIN_GROUP_NAME in databricks.yml to match your admin group.

-- Notes
-- * `system.access` and `system.billing` must be ENABLED by an account admin
--   first (Account console → Settings → System tables). Without them, lineage
--   and cost are empty regardless of these grants.
-- * Sharing views (system.information_schema.shares, table_share_usage, ...)
--   only show shares/recipients the SP is privileged on. For a full sharing
--   inventory the SP needs to be a metastore admin or be granted the relevant
--   share/recipient objects.
