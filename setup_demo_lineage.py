#!/usr/bin/env python3
"""Create demo tables, DLT pipelines, jobs, views, and MVs for lineage demonstration."""

import json
import subprocess
import sys
import time
import base64
import concurrent.futures

PROFILE = "fe-vm-vish-aws"
WAREHOUSE_ID = "9711dcb3942dac99"
CATALOG = "ws_us_e2_vish_aws_catalog"
RAW = f"{CATALOG}.lineage_demo_raw"
CURATED = f"{CATALOG}.lineage_demo_curated"


def run_sql(sql, label="", wait="50s"):
    """Execute a single SQL statement via Databricks API."""
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": wait,
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", PROFILE, "--json", payload],
        capture_output=True, text=True, timeout=120
    )
    try:
        d = json.loads(result.stdout)
        state = d.get("status", {}).get("state", "UNKNOWN")
        err = d.get("status", {}).get("error", {}).get("message", "")
        if state == "SUCCEEDED":
            print(f"  OK: {label}")
        else:
            print(f"  FAIL: {label} — {err[:120]}")
        return state == "SUCCEEDED"
    except Exception as e:
        print(f"  ERROR: {label} — {e}")
        return False


def run_sql_batch(statements):
    """Run SQL statements sequentially."""
    ok = 0
    for label, sql in statements:
        if run_sql(sql, label):
            ok += 1
    return ok


def api_post(path, payload):
    """Generic Databricks REST API POST."""
    result = subprocess.run(
        ["databricks", "api", "post", path,
         "--profile", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True, timeout=120
    )
    try:
        return json.loads(result.stdout)
    except:
        return {"error": result.stderr}


def api_put(path, payload):
    """Generic Databricks REST API PUT."""
    result = subprocess.run(
        ["databricks", "api", "put", path,
         "--profile", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True, timeout=120
    )
    try:
        return json.loads(result.stdout)
    except:
        return {"error": result.stderr}


def api_get(path):
    """Generic Databricks REST API GET."""
    result = subprocess.run(
        ["databricks", "api", "get", path, "--profile", PROFILE],
        capture_output=True, text=True, timeout=60
    )
    try:
        return json.loads(result.stdout)
    except:
        return {"error": result.stderr}


def create_notebook(path, content):
    """Create a notebook in the workspace."""
    encoded = base64.b64encode(content.encode()).decode()
    result = api_post("/api/2.0/workspace/import", {
        "path": path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": True,
    })
    if "error_code" in result:
        print(f"  FAIL notebook {path}: {result.get('message','')[:100]}")
    else:
        print(f"  OK: notebook {path}")
    return result


# ============================================================
# STEP 1: Source tables in lineage_demo_raw
# ============================================================
print("\n=== STEP 1: Creating source tables ===")

source_tables = [
    ("customers_src", f"""CREATE OR REPLACE TABLE {RAW}.customers_src
        (customer_id INT, name STRING, email STRING, region STRING, signup_date DATE, is_active BOOLEAN)"""),
    ("orders_src", f"""CREATE OR REPLACE TABLE {RAW}.orders_src
        (order_id INT, customer_id INT, product_id INT, quantity INT, unit_price DECIMAL(10,2), order_date DATE, status STRING)"""),
    ("products_src", f"""CREATE OR REPLACE TABLE {RAW}.products_src
        (product_id INT, product_name STRING, category STRING, unit_cost DECIMAL(10,2), supplier_id INT, weight_kg DECIMAL(6,2))"""),
    ("suppliers_src", f"""CREATE OR REPLACE TABLE {RAW}.suppliers_src
        (supplier_id INT, supplier_name STRING, country STRING, rating DECIMAL(2,1), contract_start DATE)"""),
    ("shipments_src", f"""CREATE OR REPLACE TABLE {RAW}.shipments_src
        (shipment_id INT, order_id INT, carrier STRING, ship_date DATE, delivery_date DATE, cost DECIMAL(8,2))"""),
    ("inventory_src", f"""CREATE OR REPLACE TABLE {RAW}.inventory_src
        (product_id INT, warehouse STRING, quantity_on_hand INT, reorder_point INT, last_restock DATE)"""),
    ("returns_src", f"""CREATE OR REPLACE TABLE {RAW}.returns_src
        (return_id INT, order_id INT, product_id INT, reason STRING, return_date DATE, refund_amount DECIMAL(10,2))"""),
    ("clickstream_src", f"""CREATE OR REPLACE TABLE {RAW}.clickstream_src
        (event_id BIGINT, customer_id INT, product_id INT, event_type STRING, page STRING, ts TIMESTAMP)"""),
]

run_sql_batch([(label, sql) for label, sql in source_tables])

# ============================================================
# STEP 2: Seed data
# ============================================================
print("\n=== STEP 2: Seeding data ===")

seed_data = [
    ("customers_src", f"""INSERT INTO {RAW}.customers_src VALUES
        (1,'Alice Chen','alice@example.com','APAC','2024-01-15',true),
        (2,'Bob Smith','bob@example.com','NA','2024-02-20',true),
        (3,'Carol Wu','carol@example.com','APAC','2024-03-10',false),
        (4,'David Jones','david@example.com','EMEA','2024-04-05',true),
        (5,'Eva Mueller','eva@example.com','EMEA','2024-05-22',true),
        (6,'Frank Lee','frank@example.com','NA','2024-06-18',true),
        (7,'Grace Kim','grace@example.com','APAC','2024-07-30',false),
        (8,'Henry Patel','henry@example.com','EMEA','2024-08-12',true)"""),
    ("products_src", f"""INSERT INTO {RAW}.products_src VALUES
        (101,'Widget A','Electronics',12.50,1,0.5),
        (102,'Widget B','Electronics',18.75,1,0.8),
        (103,'Gadget X','Hardware',45.00,2,2.1),
        (104,'Gadget Y','Hardware',62.00,2,3.4),
        (105,'Tool Alpha','Tools',28.00,3,1.2),
        (106,'Tool Beta','Tools',35.50,3,1.8),
        (107,'Part M1','Parts',5.25,4,0.1),
        (108,'Part M2','Parts',7.80,4,0.2)"""),
    ("suppliers_src", f"""INSERT INTO {RAW}.suppliers_src VALUES
        (1,'TechCorp','China',4.5,'2023-01-01'),
        (2,'HardwarePro','Germany',4.2,'2023-03-15'),
        (3,'ToolMakers Inc','USA',3.8,'2023-06-01'),
        (4,'PartSupply Ltd','Japan',4.7,'2022-11-20')"""),
    ("orders_src", f"""INSERT INTO {RAW}.orders_src VALUES
        (1001,1,101,3,15.00,'2024-06-01','completed'),
        (1002,2,103,1,55.00,'2024-06-05','completed'),
        (1003,1,105,2,32.00,'2024-06-10','completed'),
        (1004,4,102,5,22.50,'2024-06-15','shipped'),
        (1005,5,107,20,6.50,'2024-06-20','completed'),
        (1006,6,104,1,75.00,'2024-06-25','completed'),
        (1007,3,106,3,42.00,'2024-07-01','returned'),
        (1008,8,108,10,9.50,'2024-07-05','completed'),
        (1009,2,101,2,15.00,'2024-07-10','shipped'),
        (1010,4,103,1,55.00,'2024-07-15','completed'),
        (1011,1,107,50,6.50,'2024-07-20','completed'),
        (1012,5,104,2,75.00,'2024-07-25','completed')"""),
    ("shipments_src", f"""INSERT INTO {RAW}.shipments_src VALUES
        (5001,1001,'FedEx','2024-06-02','2024-06-05',12.50),
        (5002,1002,'UPS','2024-06-06','2024-06-10',18.00),
        (5003,1003,'DHL','2024-06-11','2024-06-16',15.75),
        (5004,1004,'FedEx','2024-06-16','2024-06-20',22.00),
        (5005,1005,'UPS','2024-06-21','2024-06-24',8.50),
        (5006,1006,'DHL','2024-06-26','2024-07-01',25.00),
        (5007,1008,'FedEx','2024-07-06','2024-07-10',11.25),
        (5008,1009,'UPS','2024-07-11','2024-07-14',12.50)"""),
    ("inventory_src", f"""INSERT INTO {RAW}.inventory_src VALUES
        (101,'WH-East',150,50,'2024-06-01'),
        (102,'WH-East',80,30,'2024-06-15'),
        (103,'WH-West',25,10,'2024-05-20'),
        (104,'WH-West',40,15,'2024-06-10'),
        (105,'WH-Central',200,75,'2024-07-01'),
        (106,'WH-Central',90,40,'2024-06-25'),
        (107,'WH-East',500,200,'2024-07-10'),
        (108,'WH-West',300,100,'2024-07-05')"""),
    ("returns_src", f"""INSERT INTO {RAW}.returns_src VALUES
        (9001,1007,106,'defective','2024-07-10',126.00),
        (9002,1002,103,'wrong_item','2024-06-20',55.00)"""),
    ("clickstream_src", f"""INSERT INTO {RAW}.clickstream_src VALUES
        (1,1,101,'view','product_page','2024-06-01 10:00:00'),
        (2,1,101,'add_cart','product_page','2024-06-01 10:05:00'),
        (3,1,101,'purchase','checkout','2024-06-01 10:10:00'),
        (4,2,103,'view','product_page','2024-06-05 14:00:00'),
        (5,2,103,'purchase','checkout','2024-06-05 14:15:00'),
        (6,3,106,'view','product_page','2024-07-01 09:00:00'),
        (7,3,106,'add_cart','product_page','2024-07-01 09:05:00'),
        (8,3,106,'purchase','checkout','2024-07-01 09:10:00'),
        (9,4,102,'view','search','2024-06-15 11:00:00'),
        (10,5,107,'view','product_page','2024-06-20 16:00:00')"""),
]

run_sql_batch(seed_data)

# ============================================================
# STEP 3: Create volume and upload CSV files
# ============================================================
print("\n=== STEP 3: Creating volume ===")
run_sql(f"CREATE VOLUME IF NOT EXISTS {RAW}.raw_files", "volume raw_files")

# Upload CSV files to volume via API
print("  Uploading CSV files to volume...")
csv_files = {
    "regions.csv": "region_code,region_name,timezone\nAPAC,Asia Pacific,UTC+8\nNA,North America,UTC-5\nEMEA,Europe Middle East Africa,UTC+1\nLATAM,Latin America,UTC-3",
    "exchange_rates.csv": "currency,rate_to_usd,effective_date\nEUR,1.08,2024-07-01\nGBP,1.27,2024-07-01\nJPY,0.0064,2024-07-01\nCNY,0.14,2024-07-01",
    "product_categories.csv": "category,department,margin_target\nElectronics,Tech,0.35\nHardware,Industrial,0.28\nTools,Industrial,0.32\nParts,Supply Chain,0.45",
}
for fname, content in csv_files.items():
    vol_path = f"/Volumes/{CATALOG}/lineage_demo_raw/raw_files/{fname}"
    result = subprocess.run(
        ["databricks", "api", "put", f"/api/2.0/fs/files{vol_path}",
         "--profile", PROFILE, "--json", json.dumps(content)],
        capture_output=True, text=True, timeout=30
    )
    # Try the fs/files PUT with raw body approach
    result2 = subprocess.run(
        ["databricks", "fs", "cp", "-", vol_path, "--profile", PROFILE],
        input=content, capture_output=True, text=True, timeout=30
    )
    if result2.returncode == 0:
        print(f"  OK: uploaded {fname}")
    else:
        # Write locally then upload
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        tmp.write(content)
        tmp.close()
        result3 = subprocess.run(
            ["databricks", "fs", "cp", tmp.name, f"dbfs:{vol_path}", "--profile", PROFILE],
            capture_output=True, text=True, timeout=30
        )
        os.unlink(tmp.name)
        if result3.returncode == 0:
            print(f"  OK: uploaded {fname}")
        else:
            print(f"  WARN: could not upload {fname} — {result3.stderr[:80]}")


# ============================================================
# STEP 4: Create silver tables via SQL (cross-schema, joins)
# ============================================================
print("\n=== STEP 4: Creating silver tables in curated schema (cross-schema lineage) ===")

silver_tables = [
    ("customers_silver", f"""CREATE OR REPLACE TABLE {CURATED}.customers_silver AS
        SELECT customer_id, name, email, region, signup_date, is_active,
               CASE WHEN is_active THEN 'active' ELSE 'churned' END AS status_label,
               current_timestamp() AS etl_updated_at
        FROM {RAW}.customers_src WHERE customer_id IS NOT NULL"""),

    ("products_enriched", f"""CREATE OR REPLACE TABLE {CURATED}.products_enriched AS
        SELECT p.product_id, p.product_name, p.category, p.unit_cost, p.weight_kg,
               s.supplier_name, s.country AS supplier_country, s.rating AS supplier_rating,
               current_timestamp() AS etl_updated_at
        FROM {RAW}.products_src p JOIN {RAW}.suppliers_src s ON p.supplier_id = s.supplier_id"""),

    ("orders_enriched", f"""CREATE OR REPLACE TABLE {CURATED}.orders_enriched AS
        SELECT o.order_id, o.customer_id, c.name AS customer_name, c.region,
               o.product_id, p.product_name, p.category,
               o.quantity, o.unit_price, (o.quantity * o.unit_price) AS line_total,
               o.order_date, o.status,
               current_timestamp() AS etl_updated_at
        FROM {RAW}.orders_src o
        JOIN {RAW}.customers_src c ON o.customer_id = c.customer_id
        JOIN {RAW}.products_src p ON o.product_id = p.product_id"""),

    ("shipment_tracking", f"""CREATE OR REPLACE TABLE {CURATED}.shipment_tracking AS
        SELECT sh.shipment_id, sh.order_id, o.customer_id,
               sh.carrier, sh.ship_date, sh.delivery_date, sh.cost AS shipping_cost,
               DATEDIFF(sh.delivery_date, sh.ship_date) AS transit_days,
               o.status AS order_status,
               current_timestamp() AS etl_updated_at
        FROM {RAW}.shipments_src sh JOIN {RAW}.orders_src o ON sh.order_id = o.order_id"""),

    ("inventory_status", f"""CREATE OR REPLACE TABLE {CURATED}.inventory_status AS
        SELECT i.product_id, p.product_name, p.category,
               i.warehouse, i.quantity_on_hand, i.reorder_point,
               CASE WHEN i.quantity_on_hand <= i.reorder_point THEN true ELSE false END AS needs_reorder,
               i.last_restock,
               current_timestamp() AS etl_updated_at
        FROM {RAW}.inventory_src i JOIN {RAW}.products_src p ON i.product_id = p.product_id"""),

    ("returns_analysis", f"""CREATE OR REPLACE TABLE {CURATED}.returns_analysis AS
        SELECT r.return_id, r.order_id, r.product_id, p.product_name, p.category,
               o.customer_id, c.name AS customer_name,
               r.reason, r.return_date, r.refund_amount,
               o.unit_price * o.quantity AS original_amount,
               current_timestamp() AS etl_updated_at
        FROM {RAW}.returns_src r
        JOIN {RAW}.orders_src o ON r.order_id = o.order_id
        JOIN {RAW}.products_src p ON r.product_id = p.product_id
        JOIN {RAW}.customers_src c ON o.customer_id = c.customer_id"""),

    ("clickstream_enriched", f"""CREATE OR REPLACE TABLE {CURATED}.clickstream_enriched AS
        SELECT cs.event_id, cs.customer_id, c.name AS customer_name, c.region,
               cs.product_id, p.product_name, p.category,
               cs.event_type, cs.page, cs.ts,
               current_timestamp() AS etl_updated_at
        FROM {RAW}.clickstream_src cs
        JOIN {RAW}.customers_src c ON cs.customer_id = c.customer_id
        JOIN {RAW}.products_src p ON cs.product_id = p.product_id"""),
]

run_sql_batch(silver_tables)


# ============================================================
# STEP 5: Gold aggregate tables
# ============================================================
print("\n=== STEP 5: Creating gold aggregate tables ===")

gold_tables = [
    ("customer_orders_gold", f"""CREATE OR REPLACE TABLE {CURATED}.customer_orders_gold AS
        SELECT customer_id, customer_name, region,
               COUNT(DISTINCT order_id) AS total_orders,
               SUM(line_total) AS total_revenue,
               AVG(line_total) AS avg_order_value,
               MIN(order_date) AS first_order,
               MAX(order_date) AS last_order,
               current_timestamp() AS etl_updated_at
        FROM {CURATED}.orders_enriched GROUP BY customer_id, customer_name, region"""),

    ("revenue_daily_gold", f"""CREATE OR REPLACE TABLE {CURATED}.revenue_daily_gold AS
        SELECT order_date, region, category,
               COUNT(order_id) AS order_count,
               SUM(line_total) AS daily_revenue,
               SUM(quantity) AS units_sold,
               current_timestamp() AS etl_updated_at
        FROM {CURATED}.orders_enriched GROUP BY order_date, region, category"""),

    ("product_performance_gold", f"""CREATE OR REPLACE TABLE {CURATED}.product_performance_gold AS
        SELECT oe.product_id, oe.product_name, oe.category,
               pe.supplier_name, pe.supplier_country,
               COUNT(DISTINCT oe.order_id) AS times_ordered,
               SUM(oe.quantity) AS total_units_sold,
               SUM(oe.line_total) AS total_revenue,
               AVG(oe.line_total) AS avg_sale_value,
               current_timestamp() AS etl_updated_at
        FROM {CURATED}.orders_enriched oe
        JOIN {CURATED}.products_enriched pe ON oe.product_id = pe.product_id
        GROUP BY oe.product_id, oe.product_name, oe.category, pe.supplier_name, pe.supplier_country"""),

    ("supplier_performance_gold", f"""CREATE OR REPLACE TABLE {CURATED}.supplier_performance_gold AS
        SELECT pe.supplier_name, pe.supplier_country, pe.supplier_rating,
               COUNT(DISTINCT oe.order_id) AS total_orders,
               SUM(oe.line_total) AS total_revenue,
               COUNT(DISTINCT ra.return_id) AS total_returns,
               current_timestamp() AS etl_updated_at
        FROM {CURATED}.products_enriched pe
        LEFT JOIN {CURATED}.orders_enriched oe ON pe.product_id = oe.product_id
        LEFT JOIN {CURATED}.returns_analysis ra ON pe.product_id = ra.product_id
        GROUP BY pe.supplier_name, pe.supplier_country, pe.supplier_rating"""),

    ("shipping_analytics_gold", f"""CREATE OR REPLACE TABLE {CURATED}.shipping_analytics_gold AS
        SELECT st.carrier,
               COUNT(st.shipment_id) AS total_shipments,
               AVG(st.transit_days) AS avg_transit_days,
               SUM(st.shipping_cost) AS total_shipping_cost,
               AVG(st.shipping_cost) AS avg_shipping_cost,
               current_timestamp() AS etl_updated_at
        FROM {CURATED}.shipment_tracking st GROUP BY st.carrier"""),

    ("customer_360", f"""CREATE OR REPLACE TABLE {CURATED}.customer_360 AS
        SELECT co.customer_id, co.customer_name, co.region,
               co.total_orders, co.total_revenue, co.avg_order_value,
               co.first_order, co.last_order,
               COALESCE(click_counts.total_views, 0) AS total_page_views,
               COALESCE(click_counts.total_cart_adds, 0) AS total_cart_adds,
               COALESCE(ret.total_returns, 0) AS total_returns,
               COALESCE(ret.total_refunds, 0) AS total_refunds,
               current_timestamp() AS etl_updated_at
        FROM {CURATED}.customer_orders_gold co
        LEFT JOIN (
            SELECT customer_id,
                   SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) AS total_views,
                   SUM(CASE WHEN event_type='add_cart' THEN 1 ELSE 0 END) AS total_cart_adds
            FROM {CURATED}.clickstream_enriched GROUP BY customer_id
        ) click_counts ON co.customer_id = click_counts.customer_id
        LEFT JOIN (
            SELECT customer_id, COUNT(*) AS total_returns, SUM(refund_amount) AS total_refunds
            FROM {CURATED}.returns_analysis GROUP BY customer_id
        ) ret ON co.customer_id = ret.customer_id"""),
]

run_sql_batch(gold_tables)


# ============================================================
# STEP 6: Materialized views
# ============================================================
print("\n=== STEP 6: Creating materialized views ===")

mvs = [
    ("mv_customer_lifetime_value", f"""CREATE OR REPLACE MATERIALIZED VIEW {CURATED}.mv_customer_lifetime_value AS
        SELECT customer_id, customer_name, region, total_orders, total_revenue,
               total_revenue / GREATEST(total_orders, 1) AS avg_order_value,
               DATEDIFF(last_order, first_order) AS customer_tenure_days,
               CASE
                   WHEN total_revenue > 200 THEN 'Platinum'
                   WHEN total_revenue > 100 THEN 'Gold'
                   WHEN total_revenue > 50 THEN 'Silver'
                   ELSE 'Bronze'
               END AS tier
        FROM {CURATED}.customer_orders_gold"""),

    ("mv_daily_revenue_summary", f"""CREATE OR REPLACE MATERIALIZED VIEW {CURATED}.mv_daily_revenue_summary AS
        SELECT order_date, SUM(daily_revenue) AS total_revenue,
               SUM(order_count) AS total_orders, SUM(units_sold) AS total_units
        FROM {CURATED}.revenue_daily_gold GROUP BY order_date"""),

    ("mv_inventory_alerts", f"""CREATE OR REPLACE MATERIALIZED VIEW {CURATED}.mv_inventory_alerts AS
        SELECT inv.product_id, inv.product_name, inv.category, inv.warehouse,
               inv.quantity_on_hand, inv.reorder_point, inv.needs_reorder,
               pp.total_units_sold, pp.total_revenue
        FROM {CURATED}.inventory_status inv
        LEFT JOIN {CURATED}.product_performance_gold pp ON inv.product_id = pp.product_id
        WHERE inv.needs_reorder = true"""),
]

run_sql_batch(mvs)


# ============================================================
# STEP 7: Views
# ============================================================
print("\n=== STEP 7: Creating views ===")

views = [
    ("v_active_customers", f"""CREATE OR REPLACE VIEW {CURATED}.v_active_customers AS
        SELECT c.customer_id, c.customer_name, c.region, c.tier,
               c.total_orders, c.total_revenue
        FROM {CURATED}.mv_customer_lifetime_value c
        WHERE c.total_orders > 0"""),

    ("v_top_products", f"""CREATE OR REPLACE VIEW {CURATED}.v_top_products AS
        SELECT product_id, product_name, category, supplier_name,
               total_units_sold, total_revenue,
               RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS category_rank
        FROM {CURATED}.product_performance_gold"""),

    ("v_shipping_sla_breaches", f"""CREATE OR REPLACE VIEW {CURATED}.v_shipping_sla_breaches AS
        SELECT shipment_id, order_id, carrier, ship_date, delivery_date, transit_days
        FROM {CURATED}.shipment_tracking WHERE transit_days > 5"""),

    ("v_executive_dashboard", f"""CREATE OR REPLACE VIEW {CURATED}.v_executive_dashboard AS
        SELECT 'revenue' AS metric, CAST(SUM(daily_revenue) AS STRING) AS value
        FROM {CURATED}.revenue_daily_gold
        UNION ALL
        SELECT 'customers', CAST(COUNT(*) AS STRING)
        FROM {CURATED}.mv_customer_lifetime_value
        UNION ALL
        SELECT 'avg_transit_days', CAST(ROUND(AVG(avg_transit_days),1) AS STRING)
        FROM {CURATED}.shipping_analytics_gold
        UNION ALL
        SELECT 'low_stock_items', CAST(COUNT(*) AS STRING)
        FROM {CURATED}.mv_inventory_alerts"""),
]

run_sql_batch(views)


# ============================================================
# STEP 8: DLT Pipeline notebook + pipeline
# ============================================================
print("\n=== STEP 8: Creating DLT pipeline ===")

NB_BASE = "/Workspace/Users/3f15b5ba-dbbe-4a55-ab95-34e549bd7861/lineage_demo"

dlt_notebook = f'''# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp, when, lit

CATALOG = "{CATALOG}"
RAW = f"{{CATALOG}}.lineage_demo_raw"
CURATED = f"{{CATALOG}}.lineage_demo_curated"

# ---- Bronze: read from source tables ----
@dlt.table(comment="Bronze customers from raw source")
def customers_bronze():
    return spark.read.table(f"{{RAW}}.customers_src").withColumn("_ingested_at", current_timestamp())

@dlt.table(comment="Bronze orders from raw source")
def orders_bronze():
    return spark.read.table(f"{{RAW}}.orders_src").withColumn("_ingested_at", current_timestamp())

@dlt.table(comment="Bronze products from raw source")
def products_bronze():
    return spark.read.table(f"{{RAW}}.products_src").withColumn("_ingested_at", current_timestamp())

# ---- Silver: enriched joins ----
@dlt.table(comment="Silver order details with customer and product info")
def order_details_silver():
    orders = dlt.read("orders_bronze")
    customers = dlt.read("customers_bronze")
    products = dlt.read("products_bronze")
    return (orders
        .join(customers.select("customer_id","name","region"), "customer_id")
        .join(products.select("product_id","product_name","category"), "product_id")
        .withColumn("line_total", col("quantity") * col("unit_price"))
        .withColumn("_enriched_at", current_timestamp()))

@dlt.table(comment="Silver customer summary")
def customer_summary_silver():
    orders = dlt.read("order_details_silver")
    return (orders
        .groupBy("customer_id","name","region")
        .agg(
            {{"order_id": "count", "line_total": "sum"}}
        )
        .withColumnRenamed("count(order_id)", "order_count")
        .withColumnRenamed("sum(line_total)", "total_spend")
        .withColumn("_computed_at", current_timestamp()))

# ---- Gold: aggregations ----
@dlt.table(comment="Gold revenue by category and region")
def revenue_by_category_gold():
    details = dlt.read("order_details_silver")
    return (details
        .groupBy("category","region")
        .agg(
            {{"order_id": "count", "line_total": "sum", "quantity": "sum"}}
        )
        .withColumnRenamed("count(order_id)", "order_count")
        .withColumnRenamed("sum(line_total)", "total_revenue")
        .withColumnRenamed("sum(quantity)", "total_units")
        .withColumn("_computed_at", current_timestamp()))
'''

create_notebook(f"{NB_BASE}/dlt_pipeline", dlt_notebook)

# Create DLT pipeline
pipeline_config = {
    "name": "lineage-demo-etl",
    "catalog": CATALOG,
    "target": "lineage_demo_curated",
    "development": True,
    "continuous": False,
    "channel": "CURRENT",
    "serverless": True,
    "libraries": [
        {"notebook": {"path": f"{NB_BASE}/dlt_pipeline"}}
    ],
}

print("  Creating DLT pipeline...")
pipeline_result = api_post("/api/2.0/pipelines", pipeline_config)
pipeline_id = pipeline_result.get("pipeline_id", "")
if pipeline_id:
    print(f"  OK: pipeline created — {pipeline_id}")
else:
    print(f"  FAIL: {json.dumps(pipeline_result)[:200]}")


# ============================================================
# STEP 9: Job notebooks + jobs
# ============================================================
print("\n=== STEP 9: Creating job notebooks and jobs ===")

job1_notebook = f'''# Databricks notebook source
# Job: Refresh customer 360 with fresh clickstream aggregation
from pyspark.sql.functions import col, count, sum as _sum, current_timestamp

CURATED = "{CATALOG}.lineage_demo_curated"

# Read from multiple silver/gold tables
customer_orders = spark.read.table(f"{{CURATED}}.customer_orders_gold")
clickstream = spark.read.table(f"{{CURATED}}.clickstream_enriched")
returns = spark.read.table(f"{{CURATED}}.returns_analysis")

# Aggregate clickstream
click_agg = (clickstream
    .groupBy("customer_id")
    .agg(
        count("*").alias("total_events"),
        _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
        _sum((col("event_type") == "view").cast("int")).alias("views")
    ))

# Join and write
result = (customer_orders
    .join(click_agg, "customer_id", "left")
    .join(
        returns.groupBy("customer_id").agg(count("*").alias("returns"), _sum("refund_amount").alias("refunds")),
        "customer_id", "left"
    )
    .withColumn("_refreshed_at", current_timestamp()))

result.write.mode("overwrite").saveAsTable(f"{{CURATED}}.customer_360_job")
print(f"Written {{result.count()}} rows to customer_360_job")
'''

job2_notebook = f'''# Databricks notebook source
# Job: Generate executive report table from gold layer
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql import Row

CURATED = "{CATALOG}.lineage_demo_curated"
RAW = "{CATALOG}.lineage_demo_raw"

# Read from multiple gold tables
revenue = spark.read.table(f"{{CURATED}}.revenue_daily_gold")
shipping = spark.read.table(f"{{CURATED}}.shipping_analytics_gold")
suppliers = spark.read.table(f"{{CURATED}}.supplier_performance_gold")
inventory = spark.read.table(f"{{CURATED}}.inventory_status")

# Build summary
total_revenue = revenue.agg({{"daily_revenue": "sum"}}).collect()[0][0]
total_orders = revenue.agg({{"order_count": "sum"}}).collect()[0][0]
avg_transit = shipping.agg({{"avg_transit_days": "avg"}}).collect()[0][0]
low_stock = inventory.filter(col("needs_reorder") == True).count()

rows = [
    Row(metric="total_revenue", value=float(total_revenue or 0), report_date=str(current_timestamp)),
    Row(metric="total_orders", value=float(total_orders or 0), report_date=str(current_timestamp)),
    Row(metric="avg_transit_days", value=float(avg_transit or 0), report_date=str(current_timestamp)),
    Row(metric="low_stock_items", value=float(low_stock or 0), report_date=str(current_timestamp)),
]
report_df = spark.createDataFrame(rows).withColumn("_generated_at", current_timestamp())
report_df.write.mode("overwrite").saveAsTable(f"{{CURATED}}.executive_report_job")

# Also write a copy to raw for cross-schema lineage
report_df.write.mode("overwrite").saveAsTable(f"{{RAW}}.executive_snapshot")
print("Executive report generated")
'''

create_notebook(f"{NB_BASE}/job_customer_360_refresh", job1_notebook)
create_notebook(f"{NB_BASE}/job_executive_report", job2_notebook)

# Create jobs with serverless compute
for job_name, nb_path in [
    ("lineage-demo-customer-360-refresh", f"{NB_BASE}/job_customer_360_refresh"),
    ("lineage-demo-executive-report", f"{NB_BASE}/job_executive_report"),
]:
    job_config = {
        "name": job_name,
        "tasks": [{
            "task_key": "main",
            "notebook_task": {"notebook_path": nb_path},
            "environment_key": "Default",
        }],
        "environments": [{
            "environment_key": "Default",
            "spec": {"client": "1"},
        }],
    }

    result = api_post("/api/2.0/jobs/create", job_config)
    job_id = result.get("job_id", "")
    if job_id:
        print(f"  OK: job '{job_name}' created — {job_id}")
    else:
        print(f"  FAIL: {json.dumps(result)[:200]}")


# ============================================================
# STEP 10: Trigger all runs
# ============================================================
print("\n=== STEP 10: Triggering pipeline and job runs ===")

# Start DLT pipeline
if pipeline_id:
    start = api_post(f"/api/2.0/pipelines/{pipeline_id}/updates", {"full_refresh": True})
    print(f"  Pipeline update started: {start.get('update_id','?')}")

# Run jobs
jobs_list = api_get("/api/2.1/jobs/list?limit=25")
for j in jobs_list.get("jobs", []):
    if j["settings"]["name"].startswith("lineage-demo-"):
        jid = j["job_id"]
        run = api_post("/api/2.0/jobs/run-now", {"job_id": jid})
        print(f"  Job '{j['settings']['name']}' run started: {run.get('run_id','?')}")

print("\n=== DONE ===")
print(f"Tables created in: {RAW} and {CURATED}")
print("DLT pipeline and jobs triggered — lineage will appear in system tables within ~30-60 min")
