# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Run the whole pipeline
# MAGIC Loads batch1, then batch2, then tries batch2 a second time (it must be skipped), then runs the checks.
# MAGIC This is the notebook a Databricks Job would run. Run `00_setup_and_generate` once before it.

# COMMAND ----------

for batch, as_of in [("batch1", "2026-01-01"), ("batch2", "2026-11-15"), ("batch2", "2026-11-15")]:
    print(batch, "->", dbutils.notebook.run("./10_load_batch", 1200, {"batch": batch, "as_of": as_of}))

# COMMAND ----------

dbutils.notebook.run("./20_checks", 600)   # raises if any data quality check fails
print("all checks passed")

# COMMAND ----------

# Evidence to look at: row counts and the SCD2 history of customers who moved.
display(spark.sql("""
SELECT 'bronze_batches' AS tbl, COUNT(*) AS n FROM workspace.sales_lakehouse.bronze_batches
UNION ALL SELECT 'silver_orders', COUNT(*) FROM workspace.sales_lakehouse.silver_orders
UNION ALL SELECT 'silver_rejected_orders', COUNT(*) FROM workspace.sales_lakehouse.silver_rejected_orders
UNION ALL SELECT 'dim_customer rows', COUNT(*) FROM workspace.sales_lakehouse.dim_customer
UNION ALL SELECT 'customers with 2 versions', COUNT(*) FROM (SELECT customer_id FROM workspace.sales_lakehouse.dim_customer GROUP BY customer_id HAVING COUNT(*)=2)
UNION ALL SELECT 'fact_sales', COUNT(*) FROM workspace.sales_lakehouse.fact_sales
"""))

# COMMAND ----------

display(spark.sql("""
SELECT customer_id, name, city, valid_from, valid_to, is_current
FROM workspace.sales_lakehouse.dim_customer
WHERE customer_id IN (SELECT customer_id FROM workspace.sales_lakehouse.dim_customer GROUP BY customer_id HAVING COUNT(*)=2)
ORDER BY customer_id, valid_from
"""))
