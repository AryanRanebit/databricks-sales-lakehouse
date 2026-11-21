# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Load one batch: bronze -> silver -> gold
# MAGIC Same logic as the local DuckDB version (`sql/` folder), written in Spark SQL on Delta tables.
# MAGIC
# MAGIC **Difference from local:** Delta has no multi-statement transactions. Instead every step is safe to retry:
# MAGIC bronze deletes the batch's rows before inserting, silver and gold use MERGE or "insert only if missing",
# MAGIC and the batch is recorded as done only at the very end.

# COMMAND ----------

dbutils.widgets.text("batch", "batch1")
dbutils.widgets.text("as_of", "2026-01-01")
batch = dbutils.widgets.get("batch")
as_of = dbutils.widgets.get("as_of")
RAW = f"/Volumes/workspace/sales_lakehouse/raw/{batch}"
spark.sql("USE workspace.sales_lakehouse")

def run(sql_text):
    """Run several ;-separated statements."""
    for stmt in [s.strip() for s in sql_text.split(";\n") if s.strip().rstrip(";")]:
        spark.sql(stmt.rstrip(";").replace("{batch}", batch).replace("{as_of}", as_of))

# COMMAND ----------

# MAGIC %md ## Setup (creates tables once)

# COMMAND ----------

run("""
CREATE TABLE IF NOT EXISTS bronze_batches (batch_id STRING, as_of DATE, loaded_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS bronze_customers   (customer_id STRING, name STRING, city STRING, signup_date STRING, batch_id STRING);
CREATE TABLE IF NOT EXISTS bronze_products    (product_id STRING, product_name STRING, category STRING, list_price STRING, batch_id STRING);
CREATE TABLE IF NOT EXISTS bronze_orders      (order_id STRING, customer_id STRING, order_date STRING, status STRING, batch_id STRING);
CREATE TABLE IF NOT EXISTS bronze_order_items (order_id STRING, product_id STRING, quantity STRING, unit_price STRING, batch_id STRING);
CREATE TABLE IF NOT EXISTS silver_customers   (customer_id INT, name STRING, city STRING, signup_date DATE);
CREATE TABLE IF NOT EXISTS silver_products    (product_id INT, product_name STRING, category STRING, list_price DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS silver_orders      (order_id INT, customer_id INT, order_date DATE, status STRING);
CREATE TABLE IF NOT EXISTS silver_order_items (order_id INT, product_id INT, quantity INT, unit_price DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS silver_rejected_orders (order_id INT, customer_id INT, order_date DATE, status STRING, reason STRING, batch_id STRING);
CREATE TABLE IF NOT EXISTS dim_customer (customer_key INT, customer_id INT, name STRING, city STRING, signup_date DATE, valid_from DATE, valid_to DATE, is_current BOOLEAN);
CREATE TABLE IF NOT EXISTS dim_product  (product_key INT, product_id INT, product_name STRING, category STRING, list_price DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS fact_sales (customer_key INT, product_key INT, date_key INT, order_id INT, status STRING, quantity INT, unit_price DECIMAL(10,2), line_amount DECIMAL(12,2));
CREATE TABLE IF NOT EXISTS dim_date AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS full_date, year(d) AS year, month(d) AS month,
       date_format(d, 'MMMM') AS month_name, dayofweek(d) AS day_of_week
FROM (SELECT explode(sequence(DATE'2026-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d)
""")

# COMMAND ----------

already = spark.sql(f"SELECT count(*) FROM bronze_batches WHERE batch_id = '{batch}'").first()[0]
print("batch", batch, "already loaded" if already else "not loaded yet")
if already:
    dbutils.notebook.exit(f"{batch} already loaded, skipped")

# COMMAND ----------

# MAGIC %md ## Bronze: raw rows exactly as they arrived (all text), tagged with the batch

# COMMAND ----------

files = {
    "bronze_customers":   ("customers.csv",   "customer_id STRING, name STRING, city STRING, signup_date STRING"),
    "bronze_products":    ("products.csv",    "product_id STRING, product_name STRING, category STRING, list_price STRING"),
    "bronze_orders":      ("orders.csv",      "order_id STRING, customer_id STRING, order_date STRING, status STRING"),
    "bronze_order_items": ("order_items.csv", "order_id STRING, product_id STRING, quantity STRING, unit_price STRING"),
}
from pyspark.sql import functions as F
for table, (fname, schema) in files.items():
    spark.sql(f"DELETE FROM {table} WHERE batch_id = '{batch}'")      # makes a retry safe
    (spark.read.option("header", True).schema(schema).csv(f"{RAW}/{fname}")
        .withColumn("batch_id", F.lit(batch))
        .write.mode("append").saveAsTable(table))

# COMMAND ----------

# MAGIC %md ## Silver: clean, type, then MERGE (upsert) into the silver tables

# COMMAND ----------

run("""
CREATE OR REPLACE TEMP VIEW stg_customers AS
SELECT DISTINCT CAST(customer_id AS INT) AS customer_id, TRIM(name) AS name,
       CONCAT(UPPER(LEFT(TRIM(city), 1)), LOWER(SUBSTR(TRIM(city), 2))) AS city,
       CAST(signup_date AS DATE) AS signup_date
FROM bronze_customers WHERE batch_id = '{batch}';
CREATE OR REPLACE TEMP VIEW stg_products AS
SELECT CAST(product_id AS INT) AS product_id, TRIM(product_name) AS product_name, category,
       CAST(list_price AS DECIMAL(10,2)) AS list_price
FROM bronze_products WHERE batch_id = '{batch}';
CREATE OR REPLACE TEMP VIEW stg_orders AS
SELECT CAST(order_id AS INT) AS order_id, CAST(customer_id AS INT) AS customer_id,
       CAST(order_date AS DATE) AS order_date, status
FROM bronze_orders WHERE batch_id = '{batch}';
CREATE OR REPLACE TEMP VIEW stg_order_items AS
SELECT CAST(i.order_id AS INT) AS order_id, CAST(i.product_id AS INT) AS product_id, CAST(i.quantity AS INT) AS quantity,
       COALESCE(CAST(NULLIF(i.unit_price, '') AS DECIMAL(10,2)), CAST(p.list_price AS DECIMAL(10,2))) AS unit_price
FROM bronze_order_items i
LEFT JOIN (SELECT DISTINCT product_id, list_price FROM bronze_products WHERE batch_id = '{batch}') p
  ON i.product_id = p.product_id
WHERE i.batch_id = '{batch}';
MERGE INTO silver_customers t USING stg_customers s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET name = s.name, city = s.city, signup_date = s.signup_date
WHEN NOT MATCHED THEN INSERT (customer_id, name, city, signup_date) VALUES (s.customer_id, s.name, s.city, s.signup_date);
MERGE INTO silver_products t USING stg_products s ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET product_name = s.product_name, category = s.category, list_price = s.list_price
WHEN NOT MATCHED THEN INSERT (product_id, product_name, category, list_price) VALUES (s.product_id, s.product_name, s.category, s.list_price);
CREATE OR REPLACE TEMP VIEW stg_rejected AS
SELECT o.order_id, o.customer_id, o.order_date, o.status, 'unknown customer' AS reason, '{batch}' AS batch_id
FROM stg_orders o LEFT JOIN silver_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
MERGE INTO silver_rejected_orders t USING stg_rejected s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET status = s.status
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_date, status, reason, batch_id) VALUES (s.order_id, s.customer_id, s.order_date, s.status, s.reason, s.batch_id);
MERGE INTO silver_orders t USING (SELECT * FROM stg_orders WHERE order_id NOT IN (SELECT order_id FROM stg_rejected)) s
  ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET status = s.status
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_date, status) VALUES (s.order_id, s.customer_id, s.order_date, s.status);
INSERT INTO silver_order_items
SELECT order_id, product_id, quantity, unit_price FROM stg_order_items
WHERE order_id IN (SELECT order_id FROM silver_orders)
  AND order_id NOT IN (SELECT order_id FROM silver_order_items)
""")

# COMMAND ----------

# MAGIC %md ## Gold: star schema. dim_product = Type 1 (overwrite). dim_customer = Type 2 (keep history).

# COMMAND ----------

run("""
MERGE INTO dim_product t
USING (SELECT s.*, (SELECT COALESCE(MAX(product_key), 0) FROM dim_product) + ROW_NUMBER() OVER (ORDER BY s.product_id) AS new_key
       FROM silver_products s) s
  ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET product_name = s.product_name, category = s.category, list_price = s.list_price
WHEN NOT MATCHED THEN INSERT (product_key, product_id, product_name, category, list_price)
     VALUES (s.new_key, s.product_id, s.product_name, s.category, s.list_price);
MERGE INTO dim_customer d USING silver_customers s
  ON d.customer_id = s.customer_id AND d.is_current AND (d.city <> s.city OR d.name <> s.name)
WHEN MATCHED THEN UPDATE SET valid_to = DATE_SUB(DATE'{as_of}', 1), is_current = FALSE;
INSERT INTO dim_customer
SELECT (SELECT COALESCE(MAX(customer_key), 0) FROM dim_customer) + ROW_NUMBER() OVER (ORDER BY s.customer_id),
       s.customer_id, s.name, s.city, s.signup_date,
       CASE WHEN EXISTS (SELECT 1 FROM dim_customer x WHERE x.customer_id = s.customer_id)
            THEN DATE'{as_of}' ELSE DATE'1900-01-01' END,
       DATE'9999-12-31', TRUE
FROM silver_customers s
WHERE NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id AND d.is_current);
INSERT INTO fact_sales
SELECT dc.customer_key, dp.product_key, CAST(date_format(o.order_date, 'yyyyMMdd') AS INT),
       o.order_id, o.status, i.quantity, i.unit_price, i.quantity * i.unit_price
FROM silver_order_items i
JOIN silver_orders o ON i.order_id = o.order_id
JOIN dim_customer dc ON dc.customer_id = o.customer_id AND o.order_date BETWEEN dc.valid_from AND dc.valid_to
JOIN dim_product dp ON dp.product_id = i.product_id
WHERE o.order_id NOT IN (SELECT order_id FROM fact_sales);
MERGE INTO fact_sales f USING silver_orders o ON f.order_id = o.order_id AND f.status <> o.status
WHEN MATCHED THEN UPDATE SET status = o.status
""")

# COMMAND ----------

spark.sql(f"INSERT INTO bronze_batches VALUES ('{batch}', DATE'{as_of}', current_timestamp())")
print("loaded", batch)
