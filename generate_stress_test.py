"""
Generate 150 tables with cross-dependencies across 6 business domains.

Authentication uses the Databricks SDK unified auth — supports both:
  - Service Principal: set DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
  - Personal Access Token: set DATABRICKS_HOST, DATABRICKS_TOKEN

Usage:
    # Service Principal (recommended):
    export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
    export DATABRICKS_CLIENT_ID="your-sp-client-id"
    export DATABRICKS_CLIENT_SECRET="your-sp-secret"
    export DATABRICKS_WAREHOUSE_ID="your_warehouse_id"
    export STRESS_TEST_CATALOG="your_catalog"
    export STRESS_TEST_SCHEMA="lineage_stress_test"
    python3 generate_stress_test.py

    # Personal Access Token (alternative):
    export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
    export DATABRICKS_TOKEN="dapiXXXXXXXXXXXXXXXX"
    export DATABRICKS_WAREHOUSE_ID="your_warehouse_id"
    export STRESS_TEST_CATALOG="your_catalog"
    python3 generate_stress_test.py
"""
import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

WH = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("STRESS_TEST_CATALOG", "")
SCHEMA = os.environ.get("STRESS_TEST_SCHEMA", "lineage_stress_test")

if not all([WH, CATALOG]):
    raise SystemExit(
        "Missing required environment variables.\n"
        "Set: DATABRICKS_WAREHOUSE_ID, STRESS_TEST_CATALOG\n"
        "Auth: set DATABRICKS_HOST + either DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET (SP) "
        "or DATABRICKS_TOKEN (PAT)"
    )

# WorkspaceClient auto-detects auth from env vars (SP OAuth or PAT)
client = WorkspaceClient()
FQ = f"{CATALOG}.{SCHEMA}"


def run_sql(sql, label=""):
    try:
        resp = client.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WH,
            catalog=CATALOG,
            schema=SCHEMA,
            wait_timeout="50s",
        )
        # Poll if still pending
        for _ in range(20):
            if resp.status.state in (StatementState.SUCCEEDED, StatementState.FAILED,
                                     StatementState.CANCELED, StatementState.CLOSED):
                break
            time.sleep(3)
            resp = client.statement_execution.get_statement(resp.statement_id)

        if resp.status.state == StatementState.SUCCEEDED:
            print(f"  OK: {label}")
        else:
            err = resp.status.error.message[:80] if resp.status.error else str(resp.status.state)
            print(f"  FAIL: {label} - {err}")
    except Exception as e:
        print(f"  ERR: {label} - {str(e)[:80]}")

# ============================================================
# DOMAIN DEFINITIONS
# ============================================================
domains = {
    "ecom": {
        "bronze": {
            "raw_customers": "(customer_id INT, name STRING, email STRING, phone STRING, city STRING, state STRING, country STRING, segment STRING, signup_date DATE, source STRING, ingested_at TIMESTAMP)",
            "raw_orders": "(order_id INT, customer_id INT, order_date DATE, status STRING, total DECIMAL(12,2), currency STRING, channel STRING, promo_code STRING, ingested_at TIMESTAMP)",
            "raw_order_items": "(item_id INT, order_id INT, product_id INT, quantity INT, unit_price DECIMAL(10,2), discount DECIMAL(10,2), ingested_at TIMESTAMP)",
            "raw_products": "(product_id INT, name STRING, category STRING, subcategory STRING, brand STRING, price DECIMAL(10,2), cost DECIMAL(10,2), supplier_id INT, is_active BOOLEAN, ingested_at TIMESTAMP)",
            "raw_reviews": "(review_id INT, product_id INT, customer_id INT, rating INT, review_text STRING, review_date DATE, ingested_at TIMESTAMP)",
            "raw_returns": "(return_id INT, order_id INT, item_id INT, reason STRING, return_date DATE, refund_amount DECIMAL(10,2), ingested_at TIMESTAMP)",
            "raw_coupons": "(coupon_id INT, code STRING, discount_pct DECIMAL(5,2), valid_from DATE, valid_to DATE, max_uses INT, ingested_at TIMESTAMP)",
            "raw_wishlists": "(wishlist_id INT, customer_id INT, product_id INT, added_date DATE, ingested_at TIMESTAMP)",
        },
        "silver": {
            "cleaned_customers": "SELECT customer_id, INITCAP(name) as name, LOWER(email) as email, phone, city, state, country, UPPER(segment) as segment, signup_date, source, current_timestamp() as processed_at FROM {fq}.raw_customers WHERE email IS NOT NULL",
            "cleaned_orders": "SELECT order_id, customer_id, order_date, UPPER(status) as status, total, COALESCE(currency,'USD') as currency, channel, promo_code, current_timestamp() as processed_at FROM {fq}.raw_orders WHERE total > 0",
            "cleaned_order_items": "SELECT item_id, order_id, product_id, quantity, unit_price, COALESCE(discount,0) as discount, quantity * unit_price - COALESCE(discount,0) as line_total, current_timestamp() as processed_at FROM {fq}.raw_order_items WHERE quantity > 0",
            "cleaned_products": "SELECT product_id, TRIM(name) as name, UPPER(category) as category, UPPER(subcategory) as subcategory, brand, price, cost, price - cost as margin, supplier_id, is_active, current_timestamp() as processed_at FROM {fq}.raw_products",
            "cleaned_reviews": "SELECT review_id, product_id, customer_id, LEAST(GREATEST(rating,1),5) as rating, review_text, review_date, current_timestamp() as processed_at FROM {fq}.raw_reviews WHERE rating IS NOT NULL",
            "cleaned_returns": "SELECT return_id, order_id, item_id, UPPER(reason) as reason, return_date, refund_amount, current_timestamp() as processed_at FROM {fq}.raw_returns",
            "cleaned_coupons": "SELECT coupon_id, UPPER(code) as code, discount_pct, valid_from, valid_to, max_uses, current_timestamp() as processed_at FROM {fq}.raw_coupons",
            "cleaned_wishlists": "SELECT wishlist_id, customer_id, product_id, added_date, current_timestamp() as processed_at FROM {fq}.raw_wishlists",
        },
    },
    "finance": {
        "bronze": {
            "raw_transactions": "(txn_id INT, account_id INT, txn_date DATE, txn_type STRING, amount DECIMAL(12,2), currency STRING, category STRING, merchant STRING, ingested_at TIMESTAMP)",
            "raw_accounts": "(account_id INT, customer_id INT, account_type STRING, opened_date DATE, balance DECIMAL(14,2), credit_limit DECIMAL(14,2), status STRING, ingested_at TIMESTAMP)",
            "raw_invoices": "(invoice_id INT, customer_id INT, invoice_date DATE, due_date DATE, amount DECIMAL(12,2), status STRING, payment_terms STRING, ingested_at TIMESTAMP)",
            "raw_payments": "(payment_id INT, invoice_id INT, payment_date DATE, amount DECIMAL(12,2), method STRING, status STRING, reference STRING, ingested_at TIMESTAMP)",
            "raw_budgets": "(budget_id INT, department_id INT, fiscal_year INT, quarter INT, category STRING, planned DECIMAL(14,2), ingested_at TIMESTAMP)",
            "raw_expenses": "(expense_id INT, employee_id INT, department_id INT, expense_date DATE, category STRING, amount DECIMAL(10,2), receipt_url STRING, status STRING, ingested_at TIMESTAMP)",
        },
        "silver": {
            "cleaned_transactions": "SELECT txn_id, account_id, txn_date, UPPER(txn_type) as txn_type, amount, COALESCE(currency,'USD') as currency, category, merchant, current_timestamp() as processed_at FROM {fq}.raw_transactions WHERE amount != 0",
            "cleaned_accounts": "SELECT account_id, customer_id, UPPER(account_type) as account_type, opened_date, balance, credit_limit, UPPER(status) as status, current_timestamp() as processed_at FROM {fq}.raw_accounts",
            "cleaned_invoices": "SELECT invoice_id, customer_id, invoice_date, due_date, amount, UPPER(status) as status, payment_terms, CASE WHEN due_date < current_date() AND UPPER(status) != 'PAID' THEN true ELSE false END as is_overdue, current_timestamp() as processed_at FROM {fq}.raw_invoices",
            "cleaned_payments_fin": "SELECT payment_id, invoice_id, payment_date, amount, UPPER(method) as method, UPPER(status) as status, reference, current_timestamp() as processed_at FROM {fq}.raw_payments WHERE amount > 0",
            "cleaned_budgets": "SELECT budget_id, department_id, fiscal_year, quarter, UPPER(category) as category, planned, current_timestamp() as processed_at FROM {fq}.raw_budgets",
            "cleaned_expenses": "SELECT expense_id, employee_id, department_id, expense_date, UPPER(category) as category, amount, receipt_url, UPPER(status) as status, current_timestamp() as processed_at FROM {fq}.raw_expenses WHERE amount > 0",
        },
    },
    "hr": {
        "bronze": {
            "raw_employees": "(employee_id INT, first_name STRING, last_name STRING, email STRING, department_id INT, title STRING, hire_date DATE, salary DECIMAL(12,2), manager_id INT, location STRING, ingested_at TIMESTAMP)",
            "raw_departments": "(department_id INT, name STRING, head_id INT, budget DECIMAL(14,2), cost_center STRING, parent_dept_id INT, ingested_at TIMESTAMP)",
            "raw_attendance": "(record_id INT, employee_id INT, date DATE, check_in TIMESTAMP, check_out TIMESTAMP, status STRING, ingested_at TIMESTAMP)",
            "raw_performance": "(review_id INT, employee_id INT, reviewer_id INT, review_period STRING, score DECIMAL(3,1), comments STRING, review_date DATE, ingested_at TIMESTAMP)",
            "raw_training": "(training_id INT, employee_id INT, course_name STRING, provider STRING, start_date DATE, end_date DATE, score DECIMAL(5,2), status STRING, cost DECIMAL(10,2), ingested_at TIMESTAMP)",
            "raw_payroll": "(payroll_id INT, employee_id INT, pay_period DATE, base_pay DECIMAL(12,2), bonus DECIMAL(10,2), deductions DECIMAL(10,2), net_pay DECIMAL(12,2), ingested_at TIMESTAMP)",
        },
        "silver": {
            "cleaned_employees": "SELECT employee_id, INITCAP(first_name) as first_name, INITCAP(last_name) as last_name, LOWER(email) as email, department_id, title, hire_date, salary, manager_id, location, current_timestamp() as processed_at FROM {fq}.raw_employees",
            "cleaned_departments": "SELECT department_id, INITCAP(name) as name, head_id, budget, cost_center, parent_dept_id, current_timestamp() as processed_at FROM {fq}.raw_departments",
            "cleaned_attendance": "SELECT record_id, employee_id, date, check_in, check_out, UPPER(status) as status, CASE WHEN check_out IS NOT NULL THEN (unix_timestamp(check_out) - unix_timestamp(check_in))/3600.0 ELSE NULL END as hours_worked, current_timestamp() as processed_at FROM {fq}.raw_attendance",
            "cleaned_performance": "SELECT review_id, employee_id, reviewer_id, review_period, LEAST(GREATEST(score,0),5) as score, comments, review_date, current_timestamp() as processed_at FROM {fq}.raw_performance",
            "cleaned_training": "SELECT training_id, employee_id, course_name, provider, start_date, end_date, score, UPPER(status) as status, cost, current_timestamp() as processed_at FROM {fq}.raw_training",
            "cleaned_payroll": "SELECT payroll_id, employee_id, pay_period, base_pay, COALESCE(bonus,0) as bonus, COALESCE(deductions,0) as deductions, net_pay, current_timestamp() as processed_at FROM {fq}.raw_payroll",
        },
    },
    "marketing": {
        "bronze": {
            "raw_campaigns": "(campaign_id INT, name STRING, channel STRING, start_date DATE, end_date DATE, budget DECIMAL(12,2), target_audience STRING, status STRING, ingested_at TIMESTAMP)",
            "raw_leads": "(lead_id INT, campaign_id INT, source STRING, email STRING, name STRING, company STRING, score INT, status STRING, created_date DATE, ingested_at TIMESTAMP)",
            "raw_web_events": "(event_id BIGINT, session_id STRING, visitor_id STRING, event_type STRING, page_url STRING, referrer STRING, event_time TIMESTAMP, device STRING, ingested_at TIMESTAMP)",
            "raw_email_sends": "(send_id INT, campaign_id INT, recipient_email STRING, sent_at TIMESTAMP, opened_at TIMESTAMP, clicked_at TIMESTAMP, bounced BOOLEAN, unsubscribed BOOLEAN, ingested_at TIMESTAMP)",
            "raw_ad_spend": "(spend_id INT, campaign_id INT, platform STRING, date DATE, impressions BIGINT, clicks INT, spend DECIMAL(10,2), conversions INT, ingested_at TIMESTAMP)",
        },
        "silver": {
            "cleaned_campaigns": "SELECT campaign_id, TRIM(name) as name, UPPER(channel) as channel, start_date, end_date, budget, target_audience, UPPER(status) as status, current_timestamp() as processed_at FROM {fq}.raw_campaigns",
            "cleaned_leads": "SELECT lead_id, campaign_id, UPPER(source) as source, LOWER(email) as email, name, company, LEAST(GREATEST(score,0),100) as score, UPPER(status) as status, created_date, current_timestamp() as processed_at FROM {fq}.raw_leads",
            "cleaned_web_events": "SELECT event_id, session_id, visitor_id, UPPER(event_type) as event_type, page_url, referrer, event_time, UPPER(device) as device, current_timestamp() as processed_at FROM {fq}.raw_web_events",
            "cleaned_email_sends": "SELECT send_id, campaign_id, LOWER(recipient_email) as recipient_email, sent_at, opened_at, clicked_at, COALESCE(bounced,false) as bounced, COALESCE(unsubscribed,false) as unsubscribed, CASE WHEN opened_at IS NOT NULL THEN true ELSE false END as was_opened, CASE WHEN clicked_at IS NOT NULL THEN true ELSE false END as was_clicked, current_timestamp() as processed_at FROM {fq}.raw_email_sends",
            "cleaned_ad_spend": "SELECT spend_id, campaign_id, UPPER(platform) as platform, date, impressions, clicks, spend, conversions, CASE WHEN clicks > 0 THEN spend/clicks ELSE 0 END as cpc, CASE WHEN impressions > 0 THEN (clicks*1000.0)/impressions ELSE 0 END as ctr, current_timestamp() as processed_at FROM {fq}.raw_ad_spend",
        },
    },
    "supply": {
        "bronze": {
            "raw_suppliers": "(supplier_id INT, name STRING, contact_email STRING, country STRING, rating DECIMAL(3,1), category STRING, payment_terms STRING, ingested_at TIMESTAMP)",
            "raw_inventory": "(inventory_id INT, product_id INT, warehouse_id INT, quantity INT, reorder_point INT, last_restock DATE, ingested_at TIMESTAMP)",
            "raw_warehouses": "(warehouse_id INT, name STRING, city STRING, state STRING, country STRING, capacity INT, manager_id INT, ingested_at TIMESTAMP)",
            "raw_shipments": "(shipment_id INT, order_id INT, warehouse_id INT, carrier STRING, tracking_number STRING, ship_date DATE, delivery_date DATE, status STRING, cost DECIMAL(10,2), ingested_at TIMESTAMP)",
            "raw_purchase_orders": "(po_id INT, supplier_id INT, product_id INT, quantity INT, unit_cost DECIMAL(10,2), order_date DATE, expected_date DATE, status STRING, ingested_at TIMESTAMP)",
        },
        "silver": {
            "cleaned_suppliers": "SELECT supplier_id, TRIM(name) as name, LOWER(contact_email) as contact_email, country, rating, UPPER(category) as category, payment_terms, current_timestamp() as processed_at FROM {fq}.raw_suppliers",
            "cleaned_inventory": "SELECT inventory_id, product_id, warehouse_id, quantity, reorder_point, last_restock, CASE WHEN quantity <= reorder_point THEN true ELSE false END as needs_reorder, current_timestamp() as processed_at FROM {fq}.raw_inventory",
            "cleaned_warehouses": "SELECT warehouse_id, TRIM(name) as name, city, state, country, capacity, manager_id, current_timestamp() as processed_at FROM {fq}.raw_warehouses",
            "cleaned_shipments": "SELECT shipment_id, order_id, warehouse_id, UPPER(carrier) as carrier, tracking_number, ship_date, delivery_date, UPPER(status) as status, cost, CASE WHEN delivery_date IS NOT NULL THEN DATEDIFF(delivery_date, ship_date) ELSE NULL END as transit_days, current_timestamp() as processed_at FROM {fq}.raw_shipments",
            "cleaned_purchase_orders": "SELECT po_id, supplier_id, product_id, quantity, unit_cost, quantity * unit_cost as total_cost, order_date, expected_date, UPPER(status) as status, current_timestamp() as processed_at FROM {fq}.raw_purchase_orders",
        },
    },
    "support": {
        "bronze": {
            "raw_tickets": "(ticket_id INT, customer_id INT, subject STRING, description STRING, priority STRING, category STRING, created_at TIMESTAMP, resolved_at TIMESTAMP, agent_id INT, status STRING, ingested_at TIMESTAMP)",
            "raw_agents": "(agent_id INT, name STRING, email STRING, team STRING, skill_level STRING, hire_date DATE, ingested_at TIMESTAMP)",
            "raw_satisfaction": "(survey_id INT, ticket_id INT, customer_id INT, score INT, feedback STRING, survey_date DATE, ingested_at TIMESTAMP)",
            "raw_knowledge_base": "(article_id INT, title STRING, category STRING, content STRING, author_id INT, views INT, helpful_votes INT, created_date DATE, updated_date DATE, ingested_at TIMESTAMP)",
        },
        "silver": {
            "cleaned_tickets": "SELECT ticket_id, customer_id, subject, description, UPPER(priority) as priority, UPPER(category) as category, created_at, resolved_at, agent_id, UPPER(status) as status, CASE WHEN resolved_at IS NOT NULL THEN (unix_timestamp(resolved_at) - unix_timestamp(created_at))/3600.0 ELSE NULL END as resolution_hours, current_timestamp() as processed_at FROM {fq}.raw_tickets",
            "cleaned_agents": "SELECT agent_id, INITCAP(name) as name, LOWER(email) as email, UPPER(team) as team, UPPER(skill_level) as skill_level, hire_date, current_timestamp() as processed_at FROM {fq}.raw_agents",
            "cleaned_satisfaction": "SELECT survey_id, ticket_id, customer_id, LEAST(GREATEST(score,1),10) as score, feedback, survey_date, current_timestamp() as processed_at FROM {fq}.raw_satisfaction WHERE score IS NOT NULL",
            "cleaned_knowledge_base": "SELECT article_id, title, UPPER(category) as category, content, author_id, views, helpful_votes, created_date, updated_date, current_timestamp() as processed_at FROM {fq}.raw_knowledge_base",
        },
    },
}

# Gold tables - cross-domain joins
gold_tables = {
    # Ecom gold
    "gold_customer_orders": "SELECT c.customer_id, c.name, c.email, c.segment, o.order_id, o.order_date, o.status, o.total, o.channel, current_timestamp() as processed_at FROM {fq}.cleaned_customers c JOIN {fq}.cleaned_orders o ON c.customer_id = o.customer_id",
    "gold_order_details": "SELECT o.order_id, o.customer_id, o.order_date, oi.item_id, oi.product_id, p.name as product_name, p.category, oi.quantity, oi.unit_price, oi.line_total, current_timestamp() as processed_at FROM {fq}.cleaned_orders o JOIN {fq}.cleaned_order_items oi ON o.order_id = oi.order_id JOIN {fq}.cleaned_products p ON oi.product_id = p.product_id",
    "gold_product_performance": "SELECT p.product_id, p.name, p.category, p.brand, p.price, p.margin, AVG(r.rating) as avg_rating, COUNT(r.review_id) as review_count, current_timestamp() as processed_at FROM {fq}.cleaned_products p LEFT JOIN {fq}.cleaned_reviews r ON p.product_id = r.product_id GROUP BY p.product_id, p.name, p.category, p.brand, p.price, p.margin",
    "gold_return_analysis": "SELECT r.return_id, r.order_id, r.reason, r.refund_amount, o.customer_id, o.total as order_total, current_timestamp() as processed_at FROM {fq}.cleaned_returns r JOIN {fq}.cleaned_orders o ON r.order_id = o.order_id",
    "gold_coupon_usage": "SELECT c.coupon_id, c.code, c.discount_pct, o.order_id, o.total, o.customer_id, current_timestamp() as processed_at FROM {fq}.cleaned_coupons c JOIN {fq}.cleaned_orders o ON o.promo_code = c.code",
    # Finance gold
    "gold_account_summary": "SELECT a.account_id, a.customer_id, a.account_type, a.balance, a.credit_limit, COUNT(t.txn_id) as txn_count, SUM(t.amount) as total_volume, current_timestamp() as processed_at FROM {fq}.cleaned_accounts a LEFT JOIN {fq}.cleaned_transactions t ON a.account_id = t.account_id GROUP BY a.account_id, a.customer_id, a.account_type, a.balance, a.credit_limit",
    "gold_invoice_payments": "SELECT i.invoice_id, i.customer_id, i.amount as invoice_amount, i.status as invoice_status, i.is_overdue, p.payment_id, p.amount as paid_amount, p.method, current_timestamp() as processed_at FROM {fq}.cleaned_invoices i LEFT JOIN {fq}.cleaned_payments_fin p ON i.invoice_id = p.invoice_id",
    "gold_budget_vs_actual": "SELECT b.department_id, b.fiscal_year, b.quarter, b.category, b.planned, SUM(e.amount) as actual, b.planned - COALESCE(SUM(e.amount),0) as variance, current_timestamp() as processed_at FROM {fq}.cleaned_budgets b LEFT JOIN {fq}.cleaned_expenses e ON b.department_id = e.department_id AND b.category = e.category GROUP BY b.department_id, b.fiscal_year, b.quarter, b.category, b.planned",
    # HR gold
    "gold_employee_directory": "SELECT e.employee_id, e.first_name, e.last_name, e.email, e.title, e.hire_date, e.salary, e.location, d.name as department_name, d.cost_center, current_timestamp() as processed_at FROM {fq}.cleaned_employees e JOIN {fq}.cleaned_departments d ON e.department_id = d.department_id",
    "gold_employee_performance": "SELECT e.employee_id, e.first_name, e.last_name, e.department_id, p.score, p.review_period, p.review_date, e.salary, current_timestamp() as processed_at FROM {fq}.cleaned_employees e JOIN {fq}.cleaned_performance p ON e.employee_id = p.employee_id",
    "gold_payroll_summary": "SELECT p.employee_id, e.first_name, e.last_name, e.department_id, d.name as dept_name, p.pay_period, p.base_pay, p.bonus, p.deductions, p.net_pay, current_timestamp() as processed_at FROM {fq}.cleaned_payroll p JOIN {fq}.cleaned_employees e ON p.employee_id = e.employee_id JOIN {fq}.cleaned_departments d ON e.department_id = d.department_id",
    "gold_training_tracker": "SELECT t.training_id, t.employee_id, e.first_name, e.last_name, e.department_id, t.course_name, t.provider, t.score, t.status, t.cost, current_timestamp() as processed_at FROM {fq}.cleaned_training t JOIN {fq}.cleaned_employees e ON t.employee_id = e.employee_id",
    # Marketing gold
    "gold_campaign_performance": "SELECT c.campaign_id, c.name, c.channel, c.budget, SUM(a.spend) as total_spend, SUM(a.impressions) as total_impressions, SUM(a.clicks) as total_clicks, SUM(a.conversions) as total_conversions, COUNT(l.lead_id) as leads_generated, current_timestamp() as processed_at FROM {fq}.cleaned_campaigns c LEFT JOIN {fq}.cleaned_ad_spend a ON c.campaign_id = a.campaign_id LEFT JOIN {fq}.cleaned_leads l ON c.campaign_id = l.campaign_id GROUP BY c.campaign_id, c.name, c.channel, c.budget",
    "gold_email_analytics": "SELECT e.campaign_id, c.name as campaign_name, COUNT(e.send_id) as total_sent, SUM(CASE WHEN e.was_opened THEN 1 ELSE 0 END) as total_opened, SUM(CASE WHEN e.was_clicked THEN 1 ELSE 0 END) as total_clicked, SUM(CASE WHEN e.bounced THEN 1 ELSE 0 END) as total_bounced, current_timestamp() as processed_at FROM {fq}.cleaned_email_sends e JOIN {fq}.cleaned_campaigns c ON e.campaign_id = c.campaign_id GROUP BY e.campaign_id, c.name",
    "gold_lead_pipeline": "SELECT l.lead_id, l.campaign_id, c.name as campaign_name, l.source, l.email, l.company, l.score, l.status, l.created_date, current_timestamp() as processed_at FROM {fq}.cleaned_leads l JOIN {fq}.cleaned_campaigns c ON l.campaign_id = c.campaign_id",
    # Supply chain gold
    "gold_inventory_status": "SELECT i.product_id, p.name as product_name, p.category, w.name as warehouse_name, i.quantity, i.reorder_point, i.needs_reorder, w.city, w.country, current_timestamp() as processed_at FROM {fq}.cleaned_inventory i JOIN {fq}.cleaned_products p ON i.product_id = p.product_id JOIN {fq}.cleaned_warehouses w ON i.warehouse_id = w.warehouse_id",
    "gold_shipment_tracking": "SELECT s.shipment_id, s.order_id, o.customer_id, w.name as warehouse_name, s.carrier, s.status, s.ship_date, s.delivery_date, s.transit_days, s.cost, current_timestamp() as processed_at FROM {fq}.cleaned_shipments s JOIN {fq}.cleaned_orders o ON s.order_id = o.order_id JOIN {fq}.cleaned_warehouses w ON s.warehouse_id = w.warehouse_id",
    "gold_supplier_scorecard": "SELECT s.supplier_id, s.name, s.country, s.rating, COUNT(po.po_id) as total_pos, SUM(po.total_cost) as total_spend, AVG(CASE WHEN po.status = 'DELIVERED' THEN 1.0 ELSE 0.0 END) as delivery_rate, current_timestamp() as processed_at FROM {fq}.cleaned_suppliers s LEFT JOIN {fq}.cleaned_purchase_orders po ON s.supplier_id = po.supplier_id GROUP BY s.supplier_id, s.name, s.country, s.rating",
    "gold_procurement": "SELECT po.po_id, po.supplier_id, s.name as supplier_name, p.name as product_name, po.quantity, po.unit_cost, po.total_cost, po.status, current_timestamp() as processed_at FROM {fq}.cleaned_purchase_orders po JOIN {fq}.cleaned_suppliers s ON po.supplier_id = s.supplier_id JOIN {fq}.cleaned_products p ON po.product_id = p.product_id",
    # Support gold
    "gold_ticket_details": "SELECT t.ticket_id, t.customer_id, c.name as customer_name, t.subject, t.priority, t.category, t.status, t.resolution_hours, a.name as agent_name, a.team, current_timestamp() as processed_at FROM {fq}.cleaned_tickets t JOIN {fq}.cleaned_customers c ON t.customer_id = c.customer_id JOIN {fq}.cleaned_agents a ON t.agent_id = a.agent_id",
    "gold_agent_performance": "SELECT a.agent_id, a.name, a.team, COUNT(t.ticket_id) as tickets_handled, AVG(t.resolution_hours) as avg_resolution_hours, AVG(s.score) as avg_satisfaction, current_timestamp() as processed_at FROM {fq}.cleaned_agents a LEFT JOIN {fq}.cleaned_tickets t ON a.agent_id = t.agent_id LEFT JOIN {fq}.cleaned_satisfaction s ON t.ticket_id = s.ticket_id GROUP BY a.agent_id, a.name, a.team",
    "gold_csat_analysis": "SELECT s.survey_id, s.ticket_id, s.customer_id, c.name as customer_name, s.score, s.feedback, t.category as ticket_category, t.priority, current_timestamp() as processed_at FROM {fq}.cleaned_satisfaction s JOIN {fq}.cleaned_tickets t ON s.ticket_id = t.ticket_id JOIN {fq}.cleaned_customers c ON s.customer_id = c.customer_id",
}

# Cross-domain executive views
views = {
    "vw_customer_360": "SELECT c.customer_id, c.name, c.email, c.segment, co.total_orders, co.total_revenue, co.avg_order, a.balance as account_balance, t.open_tickets, t.avg_resolution_hrs FROM (SELECT customer_id, COUNT(order_id) as total_orders, SUM(total) as total_revenue, AVG(total) as avg_order FROM {fq}.gold_customer_orders GROUP BY customer_id) co JOIN {fq}.cleaned_customers c ON co.customer_id = c.customer_id LEFT JOIN (SELECT customer_id, balance FROM {fq}.cleaned_accounts) a ON c.customer_id = a.customer_id LEFT JOIN (SELECT customer_id, COUNT(ticket_id) as open_tickets, AVG(resolution_hours) as avg_resolution_hrs FROM {fq}.cleaned_tickets WHERE status != 'CLOSED' GROUP BY customer_id) t ON c.customer_id = t.customer_id",
    "vw_revenue_by_channel": "SELECT o.channel, COUNT(DISTINCT o.order_id) as orders, SUM(o.total) as revenue, COUNT(DISTINCT o.customer_id) as customers, AVG(o.total) as aov FROM {fq}.gold_customer_orders o GROUP BY o.channel",
    "vw_product_catalog": "SELECT pp.product_id, pp.name, pp.category, pp.brand, pp.price, pp.margin, pp.avg_rating, pp.review_count, i.total_stock, i.warehouses_stocked FROM {fq}.gold_product_performance pp LEFT JOIN (SELECT product_id, SUM(quantity) as total_stock, COUNT(DISTINCT warehouse_id) as warehouses_stocked FROM {fq}.cleaned_inventory GROUP BY product_id) i ON pp.product_id = i.product_id",
    "vw_executive_dashboard": "SELECT 'ecom' as domain, COUNT(DISTINCT customer_id) as key_metric_1, SUM(total) as key_metric_2 FROM {fq}.gold_customer_orders UNION ALL SELECT 'finance', COUNT(DISTINCT account_id), SUM(total_volume) FROM {fq}.gold_account_summary UNION ALL SELECT 'hr', COUNT(DISTINCT employee_id), SUM(salary) FROM {fq}.gold_employee_directory UNION ALL SELECT 'marketing', COUNT(DISTINCT campaign_id), SUM(total_spend) FROM {fq}.gold_campaign_performance UNION ALL SELECT 'support', COUNT(DISTINCT ticket_id), AVG(resolution_hours) FROM {fq}.gold_ticket_details",
    "vw_department_costs": "SELECT d.department_id, d.name, d.budget, ps.total_payroll, bva.planned_budget, bva.actual_spend, tc.training_cost FROM {fq}.cleaned_departments d LEFT JOIN (SELECT department_id, SUM(net_pay) as total_payroll FROM {fq}.gold_payroll_summary GROUP BY department_id) ps ON d.department_id = ps.department_id LEFT JOIN (SELECT department_id, SUM(planned) as planned_budget, SUM(actual) as actual_spend FROM {fq}.gold_budget_vs_actual GROUP BY department_id) bva ON d.department_id = bva.department_id LEFT JOIN (SELECT department_id, SUM(cost) as training_cost FROM {fq}.gold_training_tracker GROUP BY department_id) tc ON d.department_id = tc.department_id",
    "vw_supply_chain_health": "SELECT gs.supplier_id, gs.name as supplier, gs.delivery_rate, is2.products_low_stock, is2.total_stock_value FROM {fq}.gold_supplier_scorecard gs LEFT JOIN (SELECT s.supplier_id, COUNT(CASE WHEN i.needs_reorder THEN 1 END) as products_low_stock, SUM(i.quantity * p.price) as total_stock_value FROM {fq}.cleaned_suppliers s JOIN {fq}.cleaned_purchase_orders po ON s.supplier_id = po.supplier_id JOIN {fq}.cleaned_inventory i ON po.product_id = i.product_id JOIN {fq}.cleaned_products p ON i.product_id = p.product_id GROUP BY s.supplier_id) is2 ON gs.supplier_id = is2.supplier_id",
    "vw_marketing_roi": "SELECT cp.campaign_id, cp.name, cp.channel, cp.budget, cp.total_spend, cp.total_conversions, cp.leads_generated, ea.total_sent, ea.total_opened, ea.total_clicked, CASE WHEN cp.total_spend > 0 THEN cp.total_conversions / cp.total_spend ELSE 0 END as conversion_per_dollar FROM {fq}.gold_campaign_performance cp LEFT JOIN {fq}.gold_email_analytics ea ON cp.campaign_id = ea.campaign_id",
    "vw_customer_support_summary": "SELECT c.customer_id, c.name, c.segment, td.total_tickets, td.avg_resolution, cs.avg_satisfaction FROM {fq}.cleaned_customers c LEFT JOIN (SELECT customer_id, COUNT(ticket_id) as total_tickets, AVG(resolution_hours) as avg_resolution FROM {fq}.gold_ticket_details GROUP BY customer_id) td ON c.customer_id = td.customer_id LEFT JOIN (SELECT customer_id, AVG(score) as avg_satisfaction FROM {fq}.gold_csat_analysis GROUP BY customer_id) cs ON c.customer_id = cs.customer_id",
    "vw_workforce_analytics": "SELECT ed.department_name, COUNT(ed.employee_id) as headcount, AVG(ed.salary) as avg_salary, AVG(ep.score) as avg_perf_score, COUNT(DISTINCT tt.training_id) as trainings_completed, SUM(ps.net_pay) as total_payroll FROM {fq}.gold_employee_directory ed LEFT JOIN {fq}.gold_employee_performance ep ON ed.employee_id = ep.employee_id LEFT JOIN {fq}.gold_training_tracker tt ON ed.employee_id = tt.employee_id LEFT JOIN {fq}.gold_payroll_summary ps ON ed.employee_id = ps.employee_id GROUP BY ed.department_name",
    "vw_order_fulfillment": "SELECT od.order_id, od.customer_id, od.order_date, od.product_name, od.quantity, st.carrier, st.status as shipment_status, st.transit_days, st.warehouse_name FROM {fq}.gold_order_details od LEFT JOIN {fq}.gold_shipment_tracking st ON od.order_id = st.order_id",
    "vw_top_customers_enterprise": "SELECT c360.customer_id, c360.name, c360.segment, c360.total_orders, c360.total_revenue, c360.account_balance, c360.open_tickets, c360.avg_resolution_hrs, pp.favorite_category FROM {fq}.vw_customer_360 c360 LEFT JOIN (SELECT o.customer_id, od.category as favorite_category, ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY COUNT(*) DESC) as rn FROM {fq}.gold_customer_orders o JOIN {fq}.gold_order_details od ON o.order_id = od.order_id GROUP BY o.customer_id, od.category) pp ON c360.customer_id = pp.customer_id AND pp.rn = 1",
}

# ============================================================
# EXECUTION
# ============================================================
total = 0

# Phase 1: Bronze tables (empty CREATE TABLE)
print("\n=== PHASE 1: BRONZE TABLES ===")
for domain, domain_data in domains.items():
    for tname, cols in domain_data["bronze"].items():
        run_sql(f"CREATE OR REPLACE TABLE {FQ}.{tname} {cols} USING DELTA", f"{domain}/{tname}")
        total += 1
print(f"Bronze: {total} tables")

# Phase 2: Insert minimal data into bronze (1-2 rows each so CTAS works)
print("\n=== PHASE 2: SEED DATA ===")
seed_data = {
    "raw_customers": "INSERT INTO {fq}.raw_customers VALUES (1,'Alice Smith','alice@test.com','555-0001','NYC','NY','US','ENTERPRISE','2023-01-01','CRM',current_timestamp()),(2,'Bob Jones','bob@test.com','555-0002','LA','CA','US','SMB','2023-02-01','WEB',current_timestamp()),(3,'Carol White','carol@test.com','555-0003','CHI','IL','US','MID','2023-03-01','CRM',current_timestamp())",
    "raw_orders": "INSERT INTO {fq}.raw_orders VALUES (1,1,'2024-01-10','completed',299.99,'USD','WEB','SAVE10',current_timestamp()),(2,2,'2024-01-12','completed',149.50,'USD','MOBILE',null,current_timestamp()),(3,1,'2024-01-15','shipped',599.00,'USD','WEB',null,current_timestamp())",
    "raw_order_items": "INSERT INTO {fq}.raw_order_items VALUES (1,1,101,2,99.99,10.00,current_timestamp()),(2,1,102,1,109.99,0,current_timestamp()),(3,2,103,1,149.50,0,current_timestamp())",
    "raw_products": "INSERT INTO {fq}.raw_products VALUES (101,'Widget A','ELECTRONICS','GADGETS','BrandX',99.99,45.00,1,true,current_timestamp()),(102,'Widget B','ELECTRONICS','ACCESSORIES','BrandY',109.99,52.00,1,true,current_timestamp()),(103,'Desk Pro','FURNITURE','DESKS','OfficeCo',149.50,80.00,2,true,current_timestamp())",
    "raw_reviews": "INSERT INTO {fq}.raw_reviews VALUES (1,101,1,5,'Great product!','2024-02-01',current_timestamp()),(2,102,2,3,'Okay','2024-02-05',current_timestamp())",
    "raw_returns": "INSERT INTO {fq}.raw_returns VALUES (1,2,3,'defective','2024-01-20',149.50,current_timestamp())",
    "raw_coupons": "INSERT INTO {fq}.raw_coupons VALUES (1,'SAVE10',10.00,'2024-01-01','2024-12-31',100,current_timestamp())",
    "raw_wishlists": "INSERT INTO {fq}.raw_wishlists VALUES (1,1,103,'2024-01-05',current_timestamp()),(2,2,101,'2024-01-08',current_timestamp())",
    "raw_transactions": "INSERT INTO {fq}.raw_transactions VALUES (1,1,'2024-01-10','PURCHASE',299.99,'USD','RETAIL','ShopX',current_timestamp()),(2,1,'2024-01-15','PURCHASE',599.00,'USD','RETAIL','ShopX',current_timestamp())",
    "raw_accounts": "INSERT INTO {fq}.raw_accounts VALUES (1,1,'CHECKING','2023-01-01',5420.50,null,'active',current_timestamp()),(2,2,'CREDIT','2023-02-01',-850.00,5000.00,'active',current_timestamp())",
    "raw_invoices": "INSERT INTO {fq}.raw_invoices VALUES (1,1,'2024-01-10','2024-02-10',299.99,'paid','NET30',current_timestamp()),(2,2,'2024-01-12','2024-02-12',149.50,'pending','NET30',current_timestamp())",
    "raw_payments": "INSERT INTO {fq}.raw_payments VALUES (1,1,'2024-01-15',299.99,'CREDIT_CARD','completed','REF001',current_timestamp())",
    "raw_budgets": "INSERT INTO {fq}.raw_budgets VALUES (1,1,2024,1,'OPEX',50000.00,current_timestamp()),(2,2,2024,1,'CAPEX',100000.00,current_timestamp())",
    "raw_expenses": "INSERT INTO {fq}.raw_expenses VALUES (1,1,1,'2024-01-15','TRAVEL',450.00,'receipt1.jpg','approved',current_timestamp()),(2,2,1,'2024-01-20','SOFTWARE',1200.00,'receipt2.jpg','approved',current_timestamp())",
    "raw_employees": "INSERT INTO {fq}.raw_employees VALUES (1,'john','doe','john@co.com',1,'Engineer','2022-01-15',95000.00,null,'NYC',current_timestamp()),(2,'jane','smith','jane@co.com',2,'Manager','2021-06-01',120000.00,null,'LA',current_timestamp()),(3,'bob','wilson','bob@co.com',1,'Senior Engineer','2020-03-01',115000.00,1,'NYC',current_timestamp())",
    "raw_departments": "INSERT INTO {fq}.raw_departments VALUES (1,'engineering',3,500000.00,'ENG-001',null,current_timestamp()),(2,'marketing',2,300000.00,'MKT-001',null,current_timestamp())",
    "raw_attendance": "INSERT INTO {fq}.raw_attendance VALUES (1,1,'2024-01-15',timestamp('2024-01-15 09:00:00'),timestamp('2024-01-15 17:30:00'),'PRESENT',current_timestamp())",
    "raw_performance": "INSERT INTO {fq}.raw_performance VALUES (1,1,2,'2024-Q1',4.2,'Excellent work','2024-03-30',current_timestamp()),(2,2,3,'2024-Q1',3.8,'Good leadership','2024-03-30',current_timestamp())",
    "raw_training": "INSERT INTO {fq}.raw_training VALUES (1,1,'AWS Cert','CloudGuru','2024-01-01','2024-02-01',92.5,'completed',500.00,current_timestamp())",
    "raw_payroll": "INSERT INTO {fq}.raw_payroll VALUES (1,1,'2024-01-31',7916.67,500.00,2100.00,6316.67,current_timestamp()),(2,2,'2024-01-31',10000.00,0,2800.00,7200.00,current_timestamp())",
    "raw_campaigns": "INSERT INTO {fq}.raw_campaigns VALUES (1,'Summer Sale','EMAIL','2024-06-01','2024-08-31',50000.00,'ALL','active',current_timestamp()),(2,'Brand Awareness','SOCIAL','2024-01-01','2024-12-31',100000.00,'NEW_USERS','active',current_timestamp())",
    "raw_leads": "INSERT INTO {fq}.raw_leads VALUES (1,1,'EMAIL','lead1@co.com','Lead One','Acme Inc',75,'qualified','2024-06-15',current_timestamp()),(2,2,'SOCIAL','lead2@co.com','Lead Two','Beta Corp',45,'new','2024-07-01',current_timestamp())",
    "raw_web_events": "INSERT INTO {fq}.raw_web_events VALUES (1,'sess1','vis1','PAGE_VIEW','/home','google.com',current_timestamp(),'DESKTOP',current_timestamp())",
    "raw_email_sends": "INSERT INTO {fq}.raw_email_sends VALUES (1,1,'alice@test.com',timestamp('2024-06-15 10:00:00'),timestamp('2024-06-15 14:00:00'),timestamp('2024-06-15 14:05:00'),false,false,current_timestamp()),(2,1,'bob@test.com',timestamp('2024-06-15 10:00:00'),null,null,false,false,current_timestamp())",
    "raw_ad_spend": "INSERT INTO {fq}.raw_ad_spend VALUES (1,2,'FACEBOOK','2024-01-15',50000,1200,450.00,25,current_timestamp()),(2,2,'GOOGLE','2024-01-15',80000,2500,800.00,60,current_timestamp())",
    "raw_suppliers": "INSERT INTO {fq}.raw_suppliers VALUES (1,'TechCorp','tech@supplier.com','US',4.5,'ELECTRONICS','NET30',current_timestamp()),(2,'OfficePlus','office@supplier.com','US',4.0,'FURNITURE','NET45',current_timestamp())",
    "raw_inventory": "INSERT INTO {fq}.raw_inventory VALUES (1,101,1,250,50,'2024-01-01',current_timestamp()),(2,102,1,100,25,'2024-01-05',current_timestamp()),(3,103,2,30,10,'2024-01-10',current_timestamp())",
    "raw_warehouses": "INSERT INTO {fq}.raw_warehouses VALUES (1,'East Hub','NYC','NY','US',10000,3,current_timestamp()),(2,'West Hub','LA','CA','US',8000,2,current_timestamp())",
    "raw_shipments": "INSERT INTO {fq}.raw_shipments VALUES (1,1,1,'FEDEX','FX123','2024-01-11','2024-01-14','delivered',12.50,current_timestamp()),(2,3,1,'UPS','UP456','2024-01-16',null,'in_transit',15.00,current_timestamp())",
    "raw_purchase_orders": "INSERT INTO {fq}.raw_purchase_orders VALUES (1,1,101,500,45.00,'2024-01-01','2024-01-15','delivered',current_timestamp()),(2,2,103,100,80.00,'2024-01-05','2024-01-20','delivered',current_timestamp())",
    "raw_tickets": "INSERT INTO {fq}.raw_tickets VALUES (1,1,'Order issue','My order is late','high','ORDER',timestamp('2024-01-20 10:00:00'),timestamp('2024-01-20 14:00:00'),1,'closed',current_timestamp()),(2,2,'Return help','Want to return','medium','RETURN',timestamp('2024-01-22 09:00:00'),null,2,'open',current_timestamp())",
    "raw_agents": "INSERT INTO {fq}.raw_agents VALUES (1,'Agent Smith','smith@co.com','TIER1','SENIOR','2022-01-01',current_timestamp()),(2,'Agent Brown','brown@co.com','TIER2','JUNIOR','2023-06-01',current_timestamp())",
    "raw_satisfaction": "INSERT INTO {fq}.raw_satisfaction VALUES (1,1,1,9,'Very helpful!','2024-01-21',current_timestamp())",
    "raw_knowledge_base": "INSERT INTO {fq}.raw_knowledge_base VALUES (1,'How to Return','RETURNS','Step by step guide...',1,1500,120,'2023-06-01','2024-01-01',current_timestamp())",
}

for tname, sql in seed_data.items():
    run_sql(sql.format(fq=FQ), f"seed {tname}")
print(f"Seeded {len(seed_data)} tables")

# Phase 3: Silver tables (CTAS from bronze)
print("\n=== PHASE 3: SILVER TABLES ===")
silver_count = 0
for domain, domain_data in domains.items():
    for tname, select_sql in domain_data["silver"].items():
        run_sql(f"CREATE OR REPLACE TABLE {FQ}.{tname} AS {select_sql.format(fq=FQ)}", f"{domain}/{tname}")
        total += 1
        silver_count += 1
print(f"Silver: {silver_count} tables")

# Phase 4: Gold tables (CTAS with cross-domain joins)
print("\n=== PHASE 4: GOLD TABLES ===")
gold_count = 0
for tname, select_sql in gold_tables.items():
    run_sql(f"CREATE OR REPLACE TABLE {FQ}.{tname} AS {select_sql.format(fq=FQ)}", tname)
    total += 1
    gold_count += 1
print(f"Gold: {gold_count} tables")

# Phase 5: Views
print("\n=== PHASE 5: VIEWS ===")
view_count = 0
for vname, select_sql in views.items():
    run_sql(f"CREATE OR REPLACE VIEW {FQ}.{vname} AS {select_sql.format(fq=FQ)}", vname)
    total += 1
    view_count += 1
print(f"Views: {view_count}")

print(f"\n=== TOTAL: {total} objects created ===")
