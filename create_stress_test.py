#!/usr/bin/env python3
"""
Create a 20-level deep lineage stress test in Databricks Unity Catalog.
~63 objects with realistic fan-in dependency patterns.

IMPORTANT: All cross-domain joins use (SELECT ... LIMIT 1) subqueries
to prevent row explosion from CROSS JOINs while still capturing lineage.

Usage:
    export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
    export DATABRICKS_TOKEN="dapi..."          # or use SP OAuth env vars
    export DATABRICKS_WAREHOUSE_ID="abc123..."
    export STRESS_TEST_CATALOG="your_catalog"
    export STRESS_TEST_SCHEMA="lineage_stress_test"  # optional, has default
    python3 create_stress_test.py
"""

import os
import time
import sys
from databricks.sdk import WorkspaceClient

CATALOG = os.environ.get("STRESS_TEST_CATALOG", "")
SCHEMA = os.environ.get("STRESS_TEST_SCHEMA", "lineage_stress_test")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

if not CATALOG:
    print("ERROR: Set STRESS_TEST_CATALOG environment variable")
    sys.exit(1)
if not WAREHOUSE_ID:
    print("ERROR: Set DATABRICKS_WAREHOUSE_ID environment variable")
    sys.exit(1)

FQN = f"{CATALOG}.{SCHEMA}"

w = WorkspaceClient()  # uses DATABRICKS_HOST + token/SP env vars automatically


def run_sql(sql: str, desc: str = ""):
    """Execute SQL and wait for completion."""
    if desc:
        print(f"  -> {desc}", flush=True)
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="50s",
        )
        if result.status and result.status.state:
            state = result.status.state.value
            if state in ("PENDING", "RUNNING"):
                stmt_id = result.statement_id
                for _ in range(60):  # max 3 min
                    time.sleep(3)
                    result = w.statement_execution.get_statement(statement_id=stmt_id)
                    if result.status.state.value not in ("PENDING", "RUNNING"):
                        break
                state = result.status.state.value
            if state == "FAILED":
                err = result.status.error if result.status.error else "Unknown error"
                print(f"     FAILED: {err}", flush=True)
                return False
        return True
    except Exception as e:
        print(f"     ERROR: {e}", flush=True)
        return False


def t(name: str) -> str:
    """Return fully qualified table name."""
    return f"{FQN}.{name}"


# ============================================================
# STEP 1: Drop and recreate schema
# ============================================================
print("=" * 60, flush=True)
print("STEP 1: Drop and recreate schema", flush=True)
print("=" * 60, flush=True)
run_sql(f"DROP SCHEMA IF EXISTS {FQN} CASCADE", "Dropping schema CASCADE")
run_sql(f"CREATE SCHEMA {FQN}", "Creating schema")
print(flush=True)

# ============================================================
# LEVEL 1: Raw Ingestion (8 tables)
# ============================================================
print("=" * 60, flush=True)
print("LEVEL 1: Raw Ingestion (8 tables)", flush=True)
print("=" * 60, flush=True)

level1_stmts = [
    ("raw_clickstream CREATE", f"""
        CREATE TABLE {t('raw_clickstream')} (
            user_id INT, session_id STRING, page_url STRING,
            event_type STRING, device_type STRING, ts TIMESTAMP
        )"""),
    ("raw_clickstream INSERT", f"""
        INSERT INTO {t('raw_clickstream')} VALUES
            (1, 's001', '/home', 'pageview', 'mobile', '2024-01-01 10:00:00'),
            (2, 's002', '/products', 'click', 'desktop', '2024-01-01 11:00:00'),
            (3, 's003', '/checkout', 'purchase', 'tablet', '2024-01-01 12:00:00')"""),
    ("raw_transactions CREATE", f"""
        CREATE TABLE {t('raw_transactions')} (
            txn_id INT, user_id INT, product_id INT, amount DECIMAL(10,2),
            currency STRING, payment_method STRING, ts TIMESTAMP
        )"""),
    ("raw_transactions INSERT", f"""
        INSERT INTO {t('raw_transactions')} VALUES
            (101, 1, 501, 29.99, 'USD', 'credit_card', '2024-01-01 10:30:00'),
            (102, 2, 502, 149.99, 'USD', 'paypal', '2024-01-01 11:30:00'),
            (103, 3, 503, 9.99, 'EUR', 'debit_card', '2024-01-01 12:30:00')"""),
    ("raw_users CREATE", f"""
        CREATE TABLE {t('raw_users')} (
            user_id INT, email STRING, country STRING,
            signup_date DATE, referral_source STRING
        )"""),
    ("raw_users INSERT", f"""
        INSERT INTO {t('raw_users')} VALUES
            (1, 'alice@example.com', 'US', '2023-06-15', 'google'),
            (2, 'bob@example.com', 'UK', '2023-08-20', 'facebook'),
            (3, 'carol@example.com', 'DE', '2023-10-01', 'organic')"""),
    ("raw_products CREATE", f"""
        CREATE TABLE {t('raw_products')} (
            product_id INT, name STRING, category STRING,
            subcategory STRING, price DECIMAL(10,2), supplier_id INT
        )"""),
    ("raw_products INSERT", f"""
        INSERT INTO {t('raw_products')} VALUES
            (501, 'Widget A', 'Electronics', 'Gadgets', 29.99, 1001),
            (502, 'Widget B', 'Electronics', 'Accessories', 149.99, 1002),
            (503, 'Widget C', 'Home', 'Kitchen', 9.99, 1001)"""),
    ("raw_suppliers CREATE", f"""
        CREATE TABLE {t('raw_suppliers')} (
            supplier_id INT, name STRING, country STRING,
            rating DECIMAL(3,2), contract_start DATE
        )"""),
    ("raw_suppliers INSERT", f"""
        INSERT INTO {t('raw_suppliers')} VALUES
            (1001, 'SupplierX', 'CN', 4.50, '2022-01-01'),
            (1002, 'SupplierY', 'US', 3.80, '2022-06-01'),
            (1003, 'SupplierZ', 'DE', 4.90, '2023-01-01')"""),
    ("raw_support_tickets CREATE", f"""
        CREATE TABLE {t('raw_support_tickets')} (
            ticket_id INT, user_id INT, category STRING,
            priority STRING, created_at TIMESTAMP, resolved_at TIMESTAMP
        )"""),
    ("raw_support_tickets INSERT", f"""
        INSERT INTO {t('raw_support_tickets')} VALUES
            (2001, 1, 'billing', 'high', '2024-01-02 09:00:00', '2024-01-02 15:00:00'),
            (2002, 2, 'shipping', 'medium', '2024-01-03 10:00:00', '2024-01-04 11:00:00'),
            (2003, 3, 'product', 'low', '2024-01-04 08:00:00', NULL)"""),
    ("raw_marketing_campaigns CREATE", f"""
        CREATE TABLE {t('raw_marketing_campaigns')} (
            campaign_id INT, channel STRING, budget DECIMAL(10,2),
            start_date DATE, end_date DATE
        )"""),
    ("raw_marketing_campaigns INSERT", f"""
        INSERT INTO {t('raw_marketing_campaigns')} VALUES
            (3001, 'email', 5000.00, '2024-01-01', '2024-01-31'),
            (3002, 'social', 10000.00, '2024-01-15', '2024-02-15'),
            (3003, 'search', 7500.00, '2024-02-01', '2024-02-28')"""),
    ("raw_campaign_impressions CREATE", f"""
        CREATE TABLE {t('raw_campaign_impressions')} (
            impression_id INT, campaign_id INT, user_id INT,
            clicked BOOLEAN, ts TIMESTAMP
        )"""),
    ("raw_campaign_impressions INSERT", f"""
        INSERT INTO {t('raw_campaign_impressions')} VALUES
            (4001, 3001, 1, true, '2024-01-05 10:00:00'),
            (4002, 3001, 2, false, '2024-01-06 11:00:00'),
            (4003, 3002, 3, true, '2024-01-20 09:00:00')"""),
]

for desc, sql in level1_stmts:
    run_sql(sql, desc)
print("  Level 1 complete: 8 tables\n", flush=True)

# ============================================================
# LEVELS 2-20: All CTAS/VIEW statements
# Each entry: (table_name, sql)
# ============================================================

# Helper: wrap a table ref in LIMIT 1 subquery for cross-domain lineage
def lim1(table_name: str, alias: str) -> str:
    """Return (SELECT * FROM table LIMIT 1) alias for cross-join lineage."""
    return f"(SELECT * FROM {t(table_name)} LIMIT 1) {alias}"


all_levels = [
    # --- LEVEL 2: Cleaned (8 tables) ---
    (2, "cleaned_clickstream", f"""
        CREATE TABLE {t('cleaned_clickstream')} AS
        SELECT user_id, session_id, LOWER(TRIM(page_url)) AS page_url,
               LOWER(event_type) AS event_type, LOWER(device_type) AS device_type,
               ts, CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_clickstream')}
        WHERE user_id IS NOT NULL AND ts IS NOT NULL
    """),
    (2, "cleaned_transactions", f"""
        CREATE TABLE {t('cleaned_transactions')} AS
        SELECT txn_id, user_id, product_id,
               CASE WHEN currency = 'EUR' THEN amount * 1.1 ELSE amount END AS amount_usd,
               payment_method, ts, CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_transactions')}
        WHERE amount > 0 AND txn_id IS NOT NULL
    """),
    (2, "cleaned_users", f"""
        CREATE TABLE {t('cleaned_users')} AS
        SELECT user_id, LOWER(email) AS email, UPPER(country) AS country,
               signup_date, LOWER(referral_source) AS referral_source,
               CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_users')}
        WHERE user_id IS NOT NULL AND email IS NOT NULL
    """),
    (2, "cleaned_products", f"""
        CREATE TABLE {t('cleaned_products')} AS
        SELECT product_id, TRIM(name) AS name, LOWER(category) AS category,
               LOWER(subcategory) AS subcategory, price, supplier_id,
               CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_products')}
        WHERE product_id IS NOT NULL AND price > 0
    """),
    (2, "cleaned_suppliers", f"""
        CREATE TABLE {t('cleaned_suppliers')} AS
        SELECT supplier_id, TRIM(name) AS name, UPPER(country) AS country,
               rating, contract_start, CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_suppliers')}
        WHERE supplier_id IS NOT NULL
    """),
    (2, "cleaned_support_tickets", f"""
        CREATE TABLE {t('cleaned_support_tickets')} AS
        SELECT ticket_id, user_id, LOWER(category) AS category,
               LOWER(priority) AS priority, created_at, resolved_at,
               CASE WHEN resolved_at IS NOT NULL
                    THEN TIMESTAMPDIFF(HOUR, created_at, resolved_at)
                    ELSE NULL END AS resolution_hours,
               CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_support_tickets')}
        WHERE ticket_id IS NOT NULL
    """),
    (2, "cleaned_marketing_campaigns", f"""
        CREATE TABLE {t('cleaned_marketing_campaigns')} AS
        SELECT campaign_id, LOWER(channel) AS channel, budget,
               start_date, end_date,
               DATEDIFF(end_date, start_date) AS duration_days,
               CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_marketing_campaigns')}
        WHERE campaign_id IS NOT NULL AND budget > 0
    """),
    (2, "cleaned_campaign_impressions", f"""
        CREATE TABLE {t('cleaned_campaign_impressions')} AS
        SELECT impression_id, campaign_id, user_id, clicked, ts,
               CURRENT_TIMESTAMP() AS processed_at
        FROM {t('raw_campaign_impressions')}
        WHERE impression_id IS NOT NULL
    """),

    # --- LEVEL 3: Validated (4 tables) ---
    (3, "validated_user_activity", f"""
        CREATE TABLE {t('validated_user_activity')} AS
        SELECT c.user_id, c.session_id, c.page_url, c.event_type, c.device_type, c.ts,
               u.email, u.country, u.signup_date,
               DATEDIFF(c.ts, u.signup_date) AS days_since_signup
        FROM {t('cleaned_clickstream')} c
        LEFT JOIN {t('cleaned_users')} u ON c.user_id = u.user_id
    """),
    (3, "validated_order_data", f"""
        CREATE TABLE {t('validated_order_data')} AS
        SELECT tx.txn_id, tx.user_id, tx.product_id, tx.amount_usd, tx.payment_method, tx.ts,
               p.name AS product_name, p.category, p.price AS list_price,
               tx.amount_usd - p.price AS discount_amount
        FROM {t('cleaned_transactions')} tx
        LEFT JOIN {t('cleaned_products')} p ON tx.product_id = p.product_id
    """),
    (3, "validated_supply_chain", f"""
        CREATE TABLE {t('validated_supply_chain')} AS
        SELECT p.product_id, p.name AS product_name, p.category, p.price,
               s.supplier_id, s.name AS supplier_name, s.country AS supplier_country,
               s.rating AS supplier_rating,
               CASE WHEN s.rating >= 4.0 THEN 'preferred' ELSE 'standard' END AS supplier_tier
        FROM {t('cleaned_products')} p
        LEFT JOIN {t('cleaned_suppliers')} s ON p.supplier_id = s.supplier_id
    """),
    (3, "validated_marketing_data", f"""
        CREATE TABLE {t('validated_marketing_data')} AS
        SELECT mc.campaign_id, mc.channel, mc.budget, mc.duration_days,
               COUNT(ci.impression_id) AS total_impressions,
               SUM(CASE WHEN ci.clicked THEN 1 ELSE 0 END) AS total_clicks,
               ROUND(SUM(CASE WHEN ci.clicked THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(ci.impression_id), 0), 2) AS ctr_pct
        FROM {t('cleaned_marketing_campaigns')} mc
        LEFT JOIN {t('cleaned_campaign_impressions')} ci ON mc.campaign_id = ci.campaign_id
        GROUP BY mc.campaign_id, mc.channel, mc.budget, mc.duration_days
    """),

    # --- LEVEL 4: Enriched (4 tables) ---
    (4, "enriched_user_profiles", f"""
        CREATE TABLE {t('enriched_user_profiles')} AS
        SELECT u.user_id, u.email, u.country, u.signup_date, u.referral_source,
               COUNT(DISTINCT va.session_id) AS total_sessions,
               COUNT(va.page_url) AS total_pageviews,
               MAX(va.ts) AS last_activity_ts,
               COUNT(DISTINCT st.ticket_id) AS support_ticket_count,
               AVG(st.resolution_hours) AS avg_resolution_hours
        FROM {t('cleaned_users')} u
        LEFT JOIN {t('validated_user_activity')} va ON u.user_id = va.user_id
        LEFT JOIN {t('cleaned_support_tickets')} st ON u.user_id = st.user_id
        GROUP BY u.user_id, u.email, u.country, u.signup_date, u.referral_source
    """),
    (4, "enriched_transactions", f"""
        CREATE TABLE {t('enriched_transactions')} AS
        SELECT vo.txn_id, vo.user_id, vo.product_id, vo.amount_usd, vo.payment_method,
               vo.product_name, vo.category, vo.list_price, vo.discount_amount, vo.ts,
               ct.amount_usd AS original_amount,
               u.country AS user_country, u.referral_source
        FROM {t('validated_order_data')} vo
        LEFT JOIN {t('cleaned_transactions')} ct ON vo.txn_id = ct.txn_id
        LEFT JOIN {t('cleaned_users')} u ON vo.user_id = u.user_id
    """),
    (4, "enriched_campaigns", f"""
        CREATE TABLE {t('enriched_campaigns')} AS
        SELECT vm.campaign_id, vm.channel, vm.budget, vm.total_impressions, vm.total_clicks, vm.ctr_pct,
               u_agg.total_users AS reached_users,
               c_agg.total_sessions AS attributed_sessions,
               vm.budget / NULLIF(vm.total_clicks, 0) AS cost_per_click
        FROM {t('validated_marketing_data')} vm
        LEFT JOIN (SELECT COUNT(DISTINCT user_id) AS total_users FROM {t('cleaned_users')}) u_agg ON 1=1
        LEFT JOIN (SELECT COUNT(DISTINCT session_id) AS total_sessions FROM {t('cleaned_clickstream')}) c_agg ON 1=1
    """),
    (4, "enriched_product_catalog", f"""
        CREATE TABLE {t('enriched_product_catalog')} AS
        SELECT vs.product_id, vs.product_name, vs.category, vs.price,
               vs.supplier_name, vs.supplier_country, vs.supplier_rating, vs.supplier_tier,
               p.subcategory,
               s.contract_start AS supplier_contract_start
        FROM {t('validated_supply_chain')} vs
        LEFT JOIN {t('cleaned_products')} p ON vs.product_id = p.product_id
        LEFT JOIN {t('cleaned_suppliers')} s ON vs.supplier_id = s.supplier_id
    """),

    # --- LEVEL 5: Sessionized/Windowed (3 tables) ---
    (5, "sessionized_clickstream", f"""
        CREATE TABLE {t('sessionized_clickstream')} AS
        SELECT va.user_id, va.session_id, va.page_url, va.event_type, va.ts,
               ep.total_sessions AS user_total_sessions, ep.country,
               cc.device_type,
               ROW_NUMBER() OVER (PARTITION BY va.user_id ORDER BY va.ts) AS event_sequence
        FROM {t('validated_user_activity')} va
        LEFT JOIN {t('enriched_user_profiles')} ep ON va.user_id = ep.user_id
        LEFT JOIN {t('cleaned_clickstream')} cc ON va.session_id = cc.session_id AND va.user_id = cc.user_id
    """),
    (5, "user_transaction_history", f"""
        CREATE TABLE {t('user_transaction_history')} AS
        SELECT et.user_id, et.txn_id, et.amount_usd, et.product_name, et.category, et.ts,
               ep.total_sessions, ep.support_ticket_count,
               ct.payment_method,
               SUM(et.amount_usd) OVER (PARTITION BY et.user_id ORDER BY et.ts) AS cumulative_spend,
               ROW_NUMBER() OVER (PARTITION BY et.user_id ORDER BY et.ts) AS txn_sequence
        FROM {t('enriched_transactions')} et
        LEFT JOIN {t('enriched_user_profiles')} ep ON et.user_id = ep.user_id
        LEFT JOIN {t('cleaned_transactions')} ct ON et.txn_id = ct.txn_id
    """),
    (5, "campaign_attribution", f"""
        CREATE TABLE {t('campaign_attribution')} AS
        SELECT ec.campaign_id, ec.channel, ec.budget, ec.total_clicks,
               va.user_id, va.event_type,
               et.txn_id, et.amount_usd AS attributed_revenue,
               CASE WHEN et.txn_id IS NOT NULL THEN 'converted' ELSE 'non_converted' END AS conversion_status
        FROM {t('enriched_campaigns')} ec
        LEFT JOIN (SELECT DISTINCT user_id, event_type FROM {t('validated_user_activity')}) va ON 1=1
        LEFT JOIN {t('enriched_transactions')} et ON va.user_id = et.user_id
    """),

    # --- LEVEL 6: Metrics Base (3 tables) ---
    (6, "user_engagement_metrics", f"""
        CREATE TABLE {t('user_engagement_metrics')} AS
        SELECT sc.user_id,
               COUNT(DISTINCT sc.session_id) AS sessions,
               COUNT(sc.page_url) AS pageviews,
               MAX(uth.cumulative_spend) AS total_spend,
               ep.support_ticket_count,
               ep.referral_source,
               CASE WHEN COUNT(DISTINCT sc.session_id) > 2 THEN 'active' ELSE 'passive' END AS engagement_level
        FROM {t('sessionized_clickstream')} sc
        LEFT JOIN {t('user_transaction_history')} uth ON sc.user_id = uth.user_id
        LEFT JOIN {t('enriched_user_profiles')} ep ON sc.user_id = ep.user_id
        GROUP BY sc.user_id, ep.support_ticket_count, ep.referral_source
    """),
    (6, "product_performance_metrics", f"""
        CREATE TABLE {t('product_performance_metrics')} AS
        SELECT et.product_id, et.product_name, et.category,
               COUNT(et.txn_id) AS total_orders,
               SUM(et.amount_usd) AS total_revenue,
               AVG(et.amount_usd) AS avg_order_value,
               epc.supplier_name, epc.supplier_tier,
               vo.list_price
        FROM {t('enriched_transactions')} et
        LEFT JOIN {t('enriched_product_catalog')} epc ON et.product_id = epc.product_id
        LEFT JOIN {t('validated_order_data')} vo ON et.txn_id = vo.txn_id
        GROUP BY et.product_id, et.product_name, et.category, epc.supplier_name, epc.supplier_tier, vo.list_price
    """),
    (6, "campaign_roi_metrics", f"""
        CREATE TABLE {t('campaign_roi_metrics')} AS
        SELECT ca.campaign_id, ca.channel, ca.budget,
               COUNT(DISTINCT ca.user_id) AS users_reached,
               SUM(CASE WHEN ca.conversion_status = 'converted' THEN 1 ELSE 0 END) AS conversions,
               SUM(ca.attributed_revenue) AS total_attributed_revenue,
               ec.cost_per_click,
               et_agg.top_country
        FROM {t('campaign_attribution')} ca
        LEFT JOIN {t('enriched_campaigns')} ec ON ca.campaign_id = ec.campaign_id
        LEFT JOIN (SELECT user_country AS top_country FROM {t('enriched_transactions')} LIMIT 1) et_agg ON 1=1
        GROUP BY ca.campaign_id, ca.channel, ca.budget, ec.cost_per_click, et_agg.top_country
    """),

    # --- LEVEL 7: Segmentation (3 tables) ---
    (7, "user_segments", f"""
        CREATE TABLE {t('user_segments')} AS
        SELECT uem.user_id, uem.sessions, uem.total_spend, uem.engagement_level,
               uth.txn_sequence AS total_transactions,
               ep.country,
               CASE
                   WHEN uem.total_spend > 100 AND uem.sessions > 2 THEN 'high_value'
                   WHEN uem.total_spend > 50 THEN 'medium_value'
                   ELSE 'low_value'
               END AS value_segment,
               CASE
                   WHEN uem.sessions > 3 THEN 'power_user'
                   WHEN uem.sessions > 1 THEN 'regular'
                   ELSE 'casual'
               END AS activity_segment
        FROM {t('user_engagement_metrics')} uem
        LEFT JOIN {t('user_transaction_history')} uth ON uem.user_id = uth.user_id
        LEFT JOIN {t('enriched_user_profiles')} ep ON uem.user_id = ep.user_id
    """),
    (7, "product_segments", f"""
        CREATE TABLE {t('product_segments')} AS
        SELECT ppm.product_id, ppm.product_name, ppm.category, ppm.total_orders, ppm.total_revenue,
               epc.supplier_tier, epc.subcategory,
               CASE
                   WHEN ppm.total_revenue > 100 THEN 'star'
                   WHEN ppm.total_orders > 1 THEN 'steady'
                   ELSE 'niche'
               END AS product_segment
        FROM {t('product_performance_metrics')} ppm
        LEFT JOIN {t('enriched_product_catalog')} epc ON ppm.product_id = epc.product_id
    """),
    (7, "campaign_segments", f"""
        CREATE TABLE {t('campaign_segments')} AS
        SELECT crm.campaign_id, crm.channel, crm.budget, crm.conversions, crm.total_attributed_revenue,
               ca_agg.top_conversion_status AS conversion_status,
               ec.ctr_pct,
               CASE
                   WHEN crm.total_attributed_revenue > crm.budget THEN 'profitable'
                   WHEN crm.conversions > 0 THEN 'break_even'
                   ELSE 'loss'
               END AS roi_segment
        FROM {t('campaign_roi_metrics')} crm
        LEFT JOIN (SELECT campaign_id, conversion_status AS top_conversion_status FROM {t('campaign_attribution')} LIMIT 3) ca_agg ON crm.campaign_id = ca_agg.campaign_id
        LEFT JOIN {t('enriched_campaigns')} ec ON crm.campaign_id = ec.campaign_id
    """),

    # --- LEVEL 8: Scoring (3 tables) ---
    (8, "user_ltv_scores", f"""
        CREATE TABLE {t('user_ltv_scores')} AS
        SELECT us.user_id, us.value_segment, us.activity_segment,
               uem.total_spend, uem.sessions,
               uth.cumulative_spend,
               ROUND(COALESCE(uem.total_spend, 0) * 12 *
                   CASE WHEN us.value_segment = 'high_value' THEN 1.5
                        WHEN us.value_segment = 'medium_value' THEN 1.0
                        ELSE 0.5 END, 2) AS estimated_annual_ltv,
               NTILE(5) OVER (ORDER BY COALESCE(uem.total_spend, 0)) AS ltv_quintile
        FROM {t('user_segments')} us
        LEFT JOIN {t('user_engagement_metrics')} uem ON us.user_id = uem.user_id
        LEFT JOIN {t('user_transaction_history')} uth ON us.user_id = uth.user_id
    """),
    (8, "product_demand_scores", f"""
        CREATE TABLE {t('product_demand_scores')} AS
        SELECT ps.product_id, ps.product_name, ps.product_segment,
               ppm.total_orders, ppm.avg_order_value,
               crm_agg.total_marketing_revenue AS marketing_attributed_revenue,
               ROUND(ppm.total_orders * ppm.avg_order_value *
                   CASE WHEN ps.product_segment = 'star' THEN 1.5 ELSE 1.0 END, 2) AS demand_score,
               NTILE(4) OVER (ORDER BY ppm.total_revenue) AS demand_quartile
        FROM {t('product_segments')} ps
        LEFT JOIN {t('product_performance_metrics')} ppm ON ps.product_id = ppm.product_id
        LEFT JOIN (SELECT SUM(total_attributed_revenue) AS total_marketing_revenue FROM {t('campaign_roi_metrics')}) crm_agg ON 1=1
    """),
    (8, "campaign_effectiveness_scores", f"""
        CREATE TABLE {t('campaign_effectiveness_scores')} AS
        SELECT cs.campaign_id, cs.channel, cs.roi_segment,
               crm.conversions, crm.total_attributed_revenue, crm.budget,
               us_agg.high_value_users,
               ROUND(COALESCE(crm.total_attributed_revenue, 0) / NULLIF(crm.budget, 0) * 100, 2) AS roi_pct,
               NTILE(3) OVER (ORDER BY COALESCE(crm.total_attributed_revenue, 0)) AS effectiveness_tier
        FROM {t('campaign_segments')} cs
        LEFT JOIN {t('campaign_roi_metrics')} crm ON cs.campaign_id = crm.campaign_id
        LEFT JOIN (
            SELECT COUNT(*) AS high_value_users FROM {t('user_segments')} WHERE value_segment = 'high_value'
        ) us_agg ON 1=1
    """),

    # --- LEVEL 9: Predictions (3 tables) ---
    (9, "churn_predictions", f"""
        CREATE TABLE {t('churn_predictions')} AS
        SELECT uls.user_id, uls.estimated_annual_ltv, uls.ltv_quintile,
               us.value_segment, us.activity_segment,
               uem.sessions, uem.engagement_level,
               sc_agg.max_event_seq AS last_event_seq,
               CASE
                   WHEN uem.engagement_level = 'passive' AND uls.ltv_quintile <= 2 THEN 0.85
                   WHEN uem.engagement_level = 'passive' THEN 0.60
                   WHEN uls.ltv_quintile <= 2 THEN 0.40
                   ELSE 0.15
               END AS churn_probability,
               CASE WHEN uem.engagement_level = 'passive' AND uls.ltv_quintile <= 2 THEN 'high_risk'
                    WHEN uem.engagement_level = 'passive' OR uls.ltv_quintile <= 2 THEN 'medium_risk'
                    ELSE 'low_risk'
               END AS churn_risk_tier
        FROM {t('user_ltv_scores')} uls
        LEFT JOIN {t('user_segments')} us ON uls.user_id = us.user_id
        LEFT JOIN {t('user_engagement_metrics')} uem ON uls.user_id = uem.user_id
        LEFT JOIN (SELECT user_id, MAX(event_sequence) AS max_event_seq FROM {t('sessionized_clickstream')} GROUP BY user_id) sc_agg ON uls.user_id = sc_agg.user_id
    """),
    (9, "demand_forecasts", f"""
        CREATE TABLE {t('demand_forecasts')} AS
        SELECT pds.product_id, pds.product_name, pds.demand_score, pds.demand_quartile,
               ps.product_segment, ps.total_orders,
               ppm.avg_order_value, ppm.total_revenue,
               ROUND(pds.demand_score * 1.1, 2) AS next_quarter_forecast,
               CASE WHEN pds.demand_quartile >= 3 THEN 'increase_stock'
                    WHEN pds.demand_quartile = 2 THEN 'maintain'
                    ELSE 'reduce_stock'
               END AS inventory_recommendation
        FROM {t('product_demand_scores')} pds
        LEFT JOIN {t('product_segments')} ps ON pds.product_id = ps.product_id
        LEFT JOIN {t('product_performance_metrics')} ppm ON pds.product_id = ppm.product_id
    """),
    (9, "campaign_response_predictions", f"""
        CREATE TABLE {t('campaign_response_predictions')} AS
        SELECT ces.campaign_id, ces.channel, ces.roi_pct, ces.effectiveness_tier,
               uls_agg.avg_ltv_quintile AS ltv_quintile,
               cs.roi_segment,
               ROUND(ces.roi_pct * CASE WHEN uls_agg.avg_ltv_quintile >= 4 THEN 1.3 ELSE 1.0 END, 2) AS predicted_roi,
               CASE WHEN ces.effectiveness_tier >= 2 AND uls_agg.avg_ltv_quintile >= 3 THEN 'scale_up'
                    WHEN ces.effectiveness_tier >= 2 THEN 'maintain'
                    ELSE 'optimize'
               END AS recommendation
        FROM {t('campaign_effectiveness_scores')} ces
        LEFT JOIN (SELECT AVG(ltv_quintile) AS avg_ltv_quintile FROM {t('user_ltv_scores')}) uls_agg ON 1=1
        LEFT JOIN {t('campaign_segments')} cs ON ces.campaign_id = cs.campaign_id
    """),

    # --- LEVEL 10: Dimensional Models (3 tables) ---
    (10, "dim_customers", f"""
        CREATE TABLE {t('dim_customers')} AS
        SELECT cp.user_id, cp.churn_probability, cp.churn_risk_tier,
               uls.estimated_annual_ltv, uls.ltv_quintile,
               us.value_segment, us.activity_segment,
               ep.email, ep.country, ep.signup_date, ep.referral_source,
               ep.total_sessions, ep.support_ticket_count
        FROM {t('churn_predictions')} cp
        LEFT JOIN {t('user_ltv_scores')} uls ON cp.user_id = uls.user_id
        LEFT JOIN {t('user_segments')} us ON cp.user_id = us.user_id
        LEFT JOIN {t('enriched_user_profiles')} ep ON cp.user_id = ep.user_id
    """),
    (10, "dim_products", f"""
        CREATE TABLE {t('dim_products')} AS
        SELECT df.product_id, df.product_name, df.demand_score, df.next_quarter_forecast,
               df.inventory_recommendation,
               pds.demand_quartile, pds.marketing_attributed_revenue,
               ps.product_segment, ps.category, ps.total_revenue,
               epc.supplier_name, epc.supplier_tier, epc.subcategory
        FROM {t('demand_forecasts')} df
        LEFT JOIN {t('product_demand_scores')} pds ON df.product_id = pds.product_id
        LEFT JOIN {t('product_segments')} ps ON df.product_id = ps.product_id
        LEFT JOIN {t('enriched_product_catalog')} epc ON df.product_id = epc.product_id
    """),
    (10, "fact_interactions", f"""
        CREATE TABLE {t('fact_interactions')} AS
        SELECT crp.campaign_id, crp.channel, crp.predicted_roi, crp.recommendation,
               cp.user_id, cp.churn_probability, cp.churn_risk_tier,
               uem.sessions, uem.total_spend, uem.engagement_level,
               crmm.conversions, crmm.total_attributed_revenue
        FROM {t('campaign_response_predictions')} crp
        LEFT JOIN (SELECT DISTINCT user_id, churn_probability, churn_risk_tier FROM {t('churn_predictions')}) cp ON 1=1
        LEFT JOIN {t('user_engagement_metrics')} uem ON cp.user_id = uem.user_id
        LEFT JOIN {t('campaign_roi_metrics')} crmm ON crp.campaign_id = crmm.campaign_id
    """),

    # --- LEVEL 11: Aggregated Facts (3 tables) ---
    (11, "fact_daily_revenue", f"""
        CREATE TABLE {t('fact_daily_revenue')} AS
        SELECT dc.country,
               dp_agg.top_category AS category,
               fi_agg.campaign_count AS campaigns,
               SUM(uth.cumulative_spend) AS total_revenue,
               AVG(dc.estimated_annual_ltv) AS avg_customer_ltv,
               COUNT(DISTINCT dc.user_id) AS customer_count
        FROM {t('dim_customers')} dc
        LEFT JOIN (SELECT category AS top_category FROM {t('dim_products')} LIMIT 1) dp_agg ON 1=1
        LEFT JOIN (SELECT COUNT(DISTINCT campaign_id) AS campaign_count FROM {t('fact_interactions')}) fi_agg ON 1=1
        LEFT JOIN {t('user_transaction_history')} uth ON dc.user_id = uth.user_id
        GROUP BY dc.country, dp_agg.top_category, fi_agg.campaign_count
    """),
    (11, "fact_daily_engagement", f"""
        CREATE TABLE {t('fact_daily_engagement')} AS
        SELECT dc.country, dc.value_segment,
               fi_agg.avg_sessions,
               fi_agg.active_count AS active_users,
               sc_agg.total_sessions,
               ca_agg.campaigns_attributed
        FROM {t('dim_customers')} dc
        LEFT JOIN (SELECT AVG(sessions) AS avg_sessions, SUM(CASE WHEN engagement_level = 'active' THEN 1 ELSE 0 END) AS active_count FROM {t('fact_interactions')}) fi_agg ON 1=1
        LEFT JOIN (SELECT COUNT(DISTINCT session_id) AS total_sessions FROM {t('sessionized_clickstream')}) sc_agg ON 1=1
        LEFT JOIN (SELECT COUNT(DISTINCT campaign_id) AS campaigns_attributed FROM {t('campaign_attribution')}) ca_agg ON 1=1
    """),
    (11, "fact_campaign_performance", f"""
        CREATE TABLE {t('fact_campaign_performance')} AS
        SELECT dc_agg.top_value_segment AS value_segment,
               dp_agg.top_product_segment AS product_segment,
               fi.campaign_id, fi.channel, fi.predicted_roi,
               fi.conversions, fi.total_attributed_revenue,
               crp.recommendation
        FROM {t('fact_interactions')} fi
        LEFT JOIN (SELECT value_segment AS top_value_segment FROM {t('dim_customers')} LIMIT 1) dc_agg ON 1=1
        LEFT JOIN (SELECT product_segment AS top_product_segment FROM {t('dim_products')} LIMIT 1) dp_agg ON 1=1
        LEFT JOIN {t('campaign_response_predictions')} crp ON fi.campaign_id = crp.campaign_id
    """),

    # --- LEVEL 12: Business KPIs (3 tables) ---
    (12, "kpi_customer_health", f"""
        CREATE TABLE {t('kpi_customer_health')} AS
        SELECT fdr.country,
               SUM(fdr.total_revenue) AS revenue,
               fde_agg.avg_engagement,
               dc_agg.top_value_segment AS value_segment,
               cp_agg.avg_churn_prob,
               CASE WHEN cp_agg.avg_churn_prob > 0.5 THEN 'unhealthy'
                    WHEN cp_agg.avg_churn_prob > 0.3 THEN 'at_risk'
                    ELSE 'healthy'
               END AS health_status
        FROM {t('fact_daily_revenue')} fdr
        LEFT JOIN (SELECT AVG(avg_sessions) AS avg_engagement FROM {t('fact_daily_engagement')}) fde_agg ON 1=1
        LEFT JOIN (SELECT value_segment AS top_value_segment FROM {t('dim_customers')} LIMIT 1) dc_agg ON 1=1
        LEFT JOIN (SELECT AVG(churn_probability) AS avg_churn_prob FROM {t('churn_predictions')}) cp_agg ON 1=1
        GROUP BY fdr.country, fde_agg.avg_engagement, dc_agg.top_value_segment, cp_agg.avg_churn_prob
    """),
    (12, "kpi_product_health", f"""
        CREATE TABLE {t('kpi_product_health')} AS
        SELECT fdr.category,
               SUM(fdr.total_revenue) AS product_revenue,
               fcp_agg.supporting_campaigns,
               dp.product_segment,
               AVG(df.next_quarter_forecast) AS avg_forecast,
               dp.inventory_recommendation
        FROM {t('fact_daily_revenue')} fdr
        LEFT JOIN (SELECT COUNT(DISTINCT campaign_id) AS supporting_campaigns FROM {t('fact_campaign_performance')}) fcp_agg ON 1=1
        LEFT JOIN {t('dim_products')} dp ON fdr.category = dp.category
        LEFT JOIN {t('demand_forecasts')} df ON dp.product_id = df.product_id
        GROUP BY fdr.category, fcp_agg.supporting_campaigns, dp.product_segment, dp.inventory_recommendation
    """),
    (12, "kpi_marketing_health", f"""
        CREATE TABLE {t('kpi_marketing_health')} AS
        SELECT fcp.channel,
               SUM(fcp.total_attributed_revenue) AS channel_revenue,
               AVG(fcp.predicted_roi) AS avg_roi,
               fde_agg.active_users,
               AVG(ces.roi_pct) AS avg_effectiveness
        FROM {t('fact_campaign_performance')} fcp
        LEFT JOIN (SELECT SUM(active_users) AS active_users FROM {t('fact_daily_engagement')}) fde_agg ON 1=1
        LEFT JOIN {t('campaign_effectiveness_scores')} ces ON fcp.campaign_id = ces.campaign_id
        GROUP BY fcp.channel, fde_agg.active_users
    """),

    # --- LEVEL 13: Trend Analysis (3 tables) ---
    (13, "trend_customer_behavior", f"""
        CREATE TABLE {t('trend_customer_behavior')} AS
        SELECT kch.country, kch.health_status, kch.revenue,
               fde.avg_sessions, fde.total_sessions,
               fdr.avg_customer_ltv,
               us_agg.top_value_segment AS value_segment,
               CASE WHEN kch.revenue > 500 THEN 'growth' ELSE 'stable' END AS trend_direction
        FROM {t('kpi_customer_health')} kch
        LEFT JOIN {t('fact_daily_engagement')} fde ON kch.country = fde.country
        LEFT JOIN {t('fact_daily_revenue')} fdr ON kch.country = fdr.country
        LEFT JOIN (SELECT value_segment AS top_value_segment FROM {t('user_segments')} LIMIT 1) us_agg ON 1=1
    """),
    (13, "trend_product_demand", f"""
        CREATE TABLE {t('trend_product_demand')} AS
        SELECT kph.category, kph.product_revenue, kph.avg_forecast, kph.product_segment,
               fdr.total_revenue AS current_revenue,
               ppm_agg.avg_order_value,
               CASE WHEN kph.avg_forecast > kph.product_revenue THEN 'increasing'
                    ELSE 'decreasing'
               END AS demand_trend
        FROM {t('kpi_product_health')} kph
        LEFT JOIN {t('fact_daily_revenue')} fdr ON kph.category = fdr.category
        LEFT JOIN (SELECT AVG(avg_order_value) AS avg_order_value FROM {t('product_performance_metrics')}) ppm_agg ON 1=1
    """),
    (13, "trend_marketing_effectiveness", f"""
        CREATE TABLE {t('trend_marketing_effectiveness')} AS
        SELECT kmh.channel, kmh.channel_revenue, kmh.avg_roi, kmh.avg_effectiveness,
               fcp_agg.avg_predicted_roi AS predicted_roi,
               crm_agg.avg_cost_per_click AS cost_per_click,
               tcb_agg.trend_direction AS customer_trend
        FROM {t('kpi_marketing_health')} kmh
        LEFT JOIN (SELECT AVG(predicted_roi) AS avg_predicted_roi FROM {t('fact_campaign_performance')}) fcp_agg ON 1=1
        LEFT JOIN (SELECT AVG(cost_per_click) AS avg_cost_per_click FROM {t('campaign_roi_metrics')}) crm_agg ON 1=1
        LEFT JOIN (SELECT trend_direction FROM {t('trend_customer_behavior')} LIMIT 1) tcb_agg ON 1=1
    """),

    # --- LEVEL 14: Anomaly Detection (2 tables) ---
    (14, "anomaly_revenue", f"""
        CREATE TABLE {t('anomaly_revenue')} AS
        SELECT tcb.country, tcb.revenue, tcb.trend_direction,
               tpd.category, tpd.demand_trend,
               fdr.total_revenue,
               kch.avg_churn_prob,
               CASE WHEN ABS(COALESCE(fdr.total_revenue, 0) - COALESCE(tcb.revenue, 0)) > 100 THEN 'anomaly'
                    ELSE 'normal'
               END AS revenue_status
        FROM {t('trend_customer_behavior')} tcb
        LEFT JOIN (SELECT category, demand_trend FROM {t('trend_product_demand')} LIMIT 1) tpd ON 1=1
        LEFT JOIN {t('fact_daily_revenue')} fdr ON tcb.country = fdr.country
        LEFT JOIN {t('kpi_customer_health')} kch ON tcb.country = kch.country
    """),
    (14, "anomaly_engagement", f"""
        CREATE TABLE {t('anomaly_engagement')} AS
        SELECT tcb.country, tcb.avg_sessions, tcb.health_status,
               tme.channel, tme.avg_roi, tme.customer_trend,
               fde.active_users, fde.total_sessions,
               kmh.avg_effectiveness,
               CASE WHEN COALESCE(fde.active_users, 0) = 0 THEN 'anomaly'
                    ELSE 'normal'
               END AS engagement_status
        FROM {t('trend_customer_behavior')} tcb
        LEFT JOIN (SELECT channel, avg_roi, customer_trend FROM {t('trend_marketing_effectiveness')} LIMIT 1) tme ON 1=1
        LEFT JOIN {t('fact_daily_engagement')} fde ON tcb.country = fde.country
        LEFT JOIN (SELECT channel, avg_effectiveness FROM {t('kpi_marketing_health')} LIMIT 1) kmh ON 1=1
    """),

    # --- LEVEL 15: Cross-Domain Analysis (2 tables) ---
    (15, "cross_domain_customer_product", f"""
        CREATE TABLE {t('cross_domain_customer_product')} AS
        SELECT ar.country, ar.revenue_status, ar.revenue,
               tcb.trend_direction, tcb.health_status,
               tpd_agg.category, tpd_agg.demand_trend, tpd_agg.product_revenue,
               dc_agg.value_segment, dc_agg.estimated_annual_ltv,
               dp_agg.product_segment, dp_agg.demand_score
        FROM {t('anomaly_revenue')} ar
        LEFT JOIN {t('trend_customer_behavior')} tcb ON ar.country = tcb.country
        LEFT JOIN (SELECT category, demand_trend, product_revenue FROM {t('trend_product_demand')} LIMIT 1) tpd_agg ON 1=1
        LEFT JOIN (SELECT value_segment, AVG(estimated_annual_ltv) AS estimated_annual_ltv FROM {t('dim_customers')} GROUP BY value_segment LIMIT 1) dc_agg ON 1=1
        LEFT JOIN (SELECT product_segment, AVG(demand_score) AS demand_score FROM {t('dim_products')} GROUP BY product_segment LIMIT 1) dp_agg ON 1=1
    """),
    (15, "cross_domain_marketing_revenue", f"""
        CREATE TABLE {t('cross_domain_marketing_revenue')} AS
        SELECT ae.channel, ae.engagement_status, ae.avg_roi,
               tme.channel_revenue, tme.customer_trend,
               ar_agg.revenue_status, ar_agg.revenue AS anomaly_revenue,
               fcp_agg.predicted_roi, fcp_agg.conversions
        FROM {t('anomaly_engagement')} ae
        LEFT JOIN {t('trend_marketing_effectiveness')} tme ON ae.channel = tme.channel
        LEFT JOIN (SELECT revenue_status, revenue FROM {t('anomaly_revenue')} LIMIT 1) ar_agg ON 1=1
        LEFT JOIN (SELECT AVG(predicted_roi) AS predicted_roi, SUM(conversions) AS conversions FROM {t('fact_campaign_performance')}) fcp_agg ON 1=1
    """),

    # --- LEVEL 16: Cohort Analysis (2 tables) ---
    (16, "cohort_customer_lifecycle", f"""
        CREATE TABLE {t('cohort_customer_lifecycle')} AS
        SELECT cdcp.country, cdcp.value_segment, cdcp.trend_direction, cdcp.demand_trend,
               kch.health_status, kch.avg_churn_prob,
               cp_agg.churn_risk_tier,
               uls_agg.estimated_annual_ltv, uls_agg.ltv_quintile,
               dc_agg.signup_date, dc_agg.referral_source,
               CASE WHEN kch.avg_churn_prob > 0.5 AND uls_agg.ltv_quintile <= 2 THEN 'at_risk_low_value'
                    WHEN kch.avg_churn_prob > 0.5 THEN 'at_risk_high_value'
                    WHEN uls_agg.ltv_quintile >= 4 THEN 'loyal_champion'
                    ELSE 'developing'
               END AS lifecycle_stage
        FROM {t('cross_domain_customer_product')} cdcp
        LEFT JOIN {t('kpi_customer_health')} kch ON cdcp.country = kch.country
        LEFT JOIN (SELECT churn_risk_tier FROM {t('churn_predictions')} LIMIT 1) cp_agg ON 1=1
        LEFT JOIN (SELECT AVG(estimated_annual_ltv) AS estimated_annual_ltv, AVG(ltv_quintile) AS ltv_quintile FROM {t('user_ltv_scores')}) uls_agg ON 1=1
        LEFT JOIN (SELECT signup_date, referral_source FROM {t('dim_customers')} LIMIT 1) dc_agg ON 1=1
    """),
    (16, "cohort_campaign_impact", f"""
        CREATE TABLE {t('cohort_campaign_impact')} AS
        SELECT cdmr.channel, cdmr.engagement_status, cdmr.channel_revenue,
               kmh.avg_effectiveness, kmh.avg_roi AS health_avg_roi,
               ces_agg.effectiveness_tier, ces_agg.roi_pct,
               fcp_agg.predicted_roi, fcp_agg.conversions,
               CASE WHEN cdmr.channel_revenue > 1000 THEN 'high_impact'
                    WHEN cdmr.channel_revenue > 500 THEN 'medium_impact'
                    ELSE 'low_impact'
               END AS campaign_impact_tier
        FROM {t('cross_domain_marketing_revenue')} cdmr
        LEFT JOIN {t('kpi_marketing_health')} kmh ON cdmr.channel = kmh.channel
        LEFT JOIN (SELECT AVG(effectiveness_tier) AS effectiveness_tier, AVG(roi_pct) AS roi_pct FROM {t('campaign_effectiveness_scores')}) ces_agg ON 1=1
        LEFT JOIN (SELECT AVG(predicted_roi) AS predicted_roi, SUM(conversions) AS conversions FROM {t('fact_campaign_performance')}) fcp_agg ON 1=1
    """),

    # --- LEVEL 17: Strategic Insights (2 tables) ---
    (17, "insight_growth_opportunities", f"""
        CREATE TABLE {t('insight_growth_opportunities')} AS
        SELECT ccl.lifecycle_stage, ccl.value_segment,
               cci_agg.campaign_impact_tier, cci_agg.channel,
               cdcp_agg.demand_trend, cdcp_agg.product_segment,
               tpd_agg.demand_trend AS product_demand_trend, tpd_agg.product_revenue,
               CASE WHEN ccl.lifecycle_stage = 'loyal_champion' AND cci_agg.campaign_impact_tier = 'high_impact'
                    THEN 'prime_expansion'
                    WHEN cdcp_agg.demand_trend = 'increasing' THEN 'product_growth'
                    ELSE 'nurture'
               END AS opportunity_type
        FROM {t('cohort_customer_lifecycle')} ccl
        LEFT JOIN (SELECT campaign_impact_tier, channel FROM {t('cohort_campaign_impact')} LIMIT 1) cci_agg ON 1=1
        LEFT JOIN (SELECT demand_trend, product_segment FROM {t('cross_domain_customer_product')} LIMIT 1) cdcp_agg ON 1=1
        LEFT JOIN (SELECT demand_trend, product_revenue FROM {t('trend_product_demand')} LIMIT 1) tpd_agg ON 1=1
    """),
    (17, "insight_risk_assessment", f"""
        CREATE TABLE {t('insight_risk_assessment')} AS
        SELECT ccl.lifecycle_stage, ccl.avg_churn_prob, ccl.churn_risk_tier,
               ar_agg.revenue_status, ar_agg.revenue,
               ae_agg.engagement_status, ae_agg.active_users,
               cp_agg.churn_probability,
               kch.health_status,
               CASE WHEN ar_agg.revenue_status = 'anomaly' AND ae_agg.engagement_status = 'anomaly'
                    THEN 'critical'
                    WHEN ar_agg.revenue_status = 'anomaly' OR ccl.avg_churn_prob > 0.6
                    THEN 'high'
                    ELSE 'moderate'
               END AS risk_level
        FROM {t('cohort_customer_lifecycle')} ccl
        LEFT JOIN (SELECT revenue_status, revenue FROM {t('anomaly_revenue')} LIMIT 1) ar_agg ON 1=1
        LEFT JOIN (SELECT engagement_status, active_users FROM {t('anomaly_engagement')} LIMIT 1) ae_agg ON 1=1
        LEFT JOIN (SELECT AVG(churn_probability) AS churn_probability FROM {t('churn_predictions')}) cp_agg ON 1=1
        LEFT JOIN {t('kpi_customer_health')} kch ON ccl.country = kch.country
    """),

    # --- LEVEL 18: Executive Aggregates (2 tables) ---
    (18, "exec_revenue_summary", f"""
        CREATE TABLE {t('exec_revenue_summary')} AS
        SELECT igo.opportunity_type,
               COUNT(*) AS opportunity_count,
               ira_agg.risk_level,
               fdr_agg.total_revenue,
               kph_agg.avg_forecast AS avg_product_forecast,
               CASE WHEN fdr_agg.total_revenue > 10000 THEN 'above_target'
                    ELSE 'below_target'
               END AS revenue_status
        FROM {t('insight_growth_opportunities')} igo
        LEFT JOIN (SELECT risk_level FROM {t('insight_risk_assessment')} LIMIT 1) ira_agg ON 1=1
        LEFT JOIN (SELECT SUM(total_revenue) AS total_revenue FROM {t('fact_daily_revenue')}) fdr_agg ON 1=1
        LEFT JOIN (SELECT AVG(avg_forecast) AS avg_forecast FROM {t('kpi_product_health')}) kph_agg ON 1=1
        GROUP BY igo.opportunity_type, ira_agg.risk_level, fdr_agg.total_revenue, kph_agg.avg_forecast
    """),
    (18, "exec_customer_summary", f"""
        CREATE TABLE {t('exec_customer_summary')} AS
        SELECT igo.opportunity_type, igo.value_segment,
               ira_agg.risk_level, ira_agg.health_status,
               kch_agg.avg_churn_prob,
               ccl_agg.lifecycle_stage,
               COUNT(*) AS cohort_size,
               CASE WHEN ira_agg.risk_level = 'critical' THEN 'immediate_action'
                    WHEN ira_agg.risk_level = 'high' THEN 'monitor_closely'
                    ELSE 'on_track'
               END AS executive_action
        FROM {t('insight_growth_opportunities')} igo
        LEFT JOIN (SELECT risk_level, health_status FROM {t('insight_risk_assessment')} LIMIT 1) ira_agg ON 1=1
        LEFT JOIN (SELECT AVG(avg_churn_prob) AS avg_churn_prob FROM {t('kpi_customer_health')}) kch_agg ON 1=1
        LEFT JOIN (SELECT lifecycle_stage FROM {t('cohort_customer_lifecycle')} LIMIT 1) ccl_agg ON 1=1
        GROUP BY igo.opportunity_type, igo.value_segment, ira_agg.risk_level, ira_agg.health_status, kch_agg.avg_churn_prob, ccl_agg.lifecycle_stage
    """),

    # --- LEVEL 19: Board Reports (1 table) ---
    (19, "board_quarterly_review", f"""
        CREATE TABLE {t('board_quarterly_review')} AS
        SELECT ers.opportunity_type, ers.total_revenue, ers.revenue_status,
               ecs.risk_level, ecs.executive_action, ecs.cohort_size,
               igo_agg.campaign_impact_tier, igo_agg.product_demand_trend,
               ira_agg.avg_churn_prob, ira_agg.risk_level AS assessed_risk,
               tpd_agg.demand_trend,
               CASE WHEN ers.revenue_status = 'above_target' AND ira_agg.risk_level = 'moderate'
                    THEN 'strong_quarter'
                    WHEN ers.revenue_status = 'below_target' AND ira_agg.risk_level = 'critical'
                    THEN 'crisis_quarter'
                    ELSE 'mixed_quarter'
               END AS quarter_assessment
        FROM {t('exec_revenue_summary')} ers
        LEFT JOIN {t('exec_customer_summary')} ecs ON ers.opportunity_type = ecs.opportunity_type
        LEFT JOIN (SELECT campaign_impact_tier, product_demand_trend FROM {t('insight_growth_opportunities')} LIMIT 1) igo_agg ON 1=1
        LEFT JOIN (SELECT risk_level, avg_churn_prob FROM {t('insight_risk_assessment')} LIMIT 1) ira_agg ON 1=1
        LEFT JOIN (SELECT demand_trend FROM {t('trend_product_demand')} LIMIT 1) tpd_agg ON 1=1
    """),

    # --- LEVEL 20: CEO Dashboard (1 view) ---
    (20, "vw_ceo_dashboard", f"""
        CREATE VIEW {t('vw_ceo_dashboard')} AS
        SELECT bqr.quarter_assessment, bqr.total_revenue, bqr.revenue_status,
               bqr.executive_action, bqr.demand_trend,
               ers.avg_product_forecast,
               ecs.avg_churn_prob AS customer_churn_risk,
               ecs.lifecycle_stage,
               kch.health_status AS customer_health,
               kph.product_segment, kph.product_revenue,
               kmh.channel AS top_channel, kmh.avg_effectiveness AS marketing_effectiveness
        FROM {t('board_quarterly_review')} bqr
        LEFT JOIN {t('exec_revenue_summary')} ers ON bqr.opportunity_type = ers.opportunity_type
        LEFT JOIN {t('exec_customer_summary')} ecs ON bqr.opportunity_type = ecs.opportunity_type
        LEFT JOIN {t('kpi_customer_health')} kch ON ecs.health_status = kch.health_status
        LEFT JOIN (SELECT product_segment, product_revenue FROM {t('kpi_product_health')} LIMIT 1) kph ON 1=1
        LEFT JOIN (SELECT channel, avg_effectiveness FROM {t('kpi_marketing_health')} LIMIT 1) kmh ON 1=1
    """),
]

# Execute all levels
current_level = 0
level_counts = {}

for level, name, sql in all_levels:
    if level != current_level:
        if current_level > 0:
            print(f"  Level {current_level} complete: {level_counts.get(current_level, 0)} objects\n", flush=True)
        current_level = level
        count_at_level = sum(1 for l, _, _ in all_levels if l == level)
        print("=" * 60, flush=True)
        print(f"LEVEL {level}: ({count_at_level} objects)", flush=True)
        print("=" * 60, flush=True)

    level_counts[level] = level_counts.get(level, 0) + 1
    run_sql(sql, f"Creating {name}")

# Print last level completion
if current_level > 0:
    print(f"  Level {current_level} complete: {level_counts.get(current_level, 0)} objects\n", flush=True)

# ============================================================
# SUMMARY
# ============================================================
print("=" * 60, flush=True)
print("SUMMARY", flush=True)
print("=" * 60, flush=True)

total = 8 + len(all_levels)  # 8 raw tables (CREATE+INSERT pairs) + all CTAS/VIEW
print(f"Total objects created: {total}", flush=True)
for lvl in sorted(level_counts.keys()):
    print(f"  Level {lvl:2d}: {level_counts[lvl]} objects", flush=True)
print(f"\nSchema: {FQN}", flush=True)
print("Done!", flush=True)
