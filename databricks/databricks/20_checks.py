# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Data quality checks and a report
# MAGIC Fails the notebook (and so the job) if any check finds bad rows.

# COMMAND ----------

spark.sql("USE workspace.sales_lakehouse")
checks = spark.sql("""
SELECT 'one current row per customer' AS check_name, COUNT(*) AS bad_rows FROM
  (SELECT customer_id FROM dim_customer WHERE is_current GROUP BY customer_id HAVING COUNT(*) <> 1)
UNION ALL SELECT 'every customer has a current row', COUNT(*) FROM silver_customers
  WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer WHERE is_current)
UNION ALL SELECT 'no overlapping customer versions', COUNT(*) FROM dim_customer a JOIN dim_customer b
  ON a.customer_id = b.customer_id AND a.customer_key < b.customer_key AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to
UNION ALL SELECT 'customer_key unique', COUNT(*) - COUNT(DISTINCT customer_key) FROM dim_customer
UNION ALL SELECT 'no null prices in fact', COUNT(*) FROM fact_sales WHERE unit_price IS NULL
UNION ALL SELECT 'no zero or negative quantity', COUNT(*) FROM fact_sales WHERE quantity <= 0
UNION ALL SELECT 'fact -> dim_customer integrity', COUNT(*) FROM fact_sales f LEFT JOIN dim_customer d ON f.customer_key = d.customer_key WHERE d.customer_key IS NULL
UNION ALL SELECT 'fact customer version valid on order date', COUNT(*) FROM fact_sales f
  JOIN dim_customer d ON f.customer_key = d.customer_key JOIN dim_date dd ON f.date_key = dd.date_key WHERE dd.full_date NOT BETWEEN d.valid_from AND d.valid_to
UNION ALL SELECT 'fact -> dim_product integrity', COUNT(*) FROM fact_sales f LEFT JOIN dim_product d ON f.product_key = d.product_key WHERE d.product_key IS NULL
UNION ALL SELECT 'fact -> dim_date integrity', COUNT(*) FROM fact_sales f LEFT JOIN dim_date d ON f.date_key = d.date_key WHERE d.date_key IS NULL
UNION ALL SELECT 'every clean order is in fact', COUNT(*) FROM silver_orders WHERE order_id NOT IN (SELECT order_id FROM fact_sales)
UNION ALL SELECT 'fact lines match silver lines', ABS((SELECT COUNT(*) FROM fact_sales) - (SELECT COUNT(*) FROM silver_order_items))
UNION ALL SELECT 'cities are tidy', COUNT(*) FROM dim_customer WHERE city <> TRIM(city) OR city <> CONCAT(UPPER(LEFT(city,1)), LOWER(SUBSTR(city,2)))
""")
display(checks)
bad = checks.filter("bad_rows <> 0").count()
assert bad == 0, f"{bad} data quality checks failed"

# COMMAND ----------

display(spark.sql("""
SELECT d.month, c.city, ROUND(SUM(f.line_amount), 2) AS revenue
FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.status = 'delivered' GROUP BY d.month, c.city ORDER BY d.month, revenue DESC
"""))
