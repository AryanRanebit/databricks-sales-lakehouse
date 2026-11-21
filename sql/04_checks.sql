-- Data quality checks. Each row: check name, number of bad rows (0 = pass).
SELECT 'one current row per customer' AS check_name, COUNT(*) AS bad_rows FROM
  (SELECT customer_id FROM dim_customer WHERE is_current GROUP BY 1 HAVING COUNT(*) <> 1)
UNION ALL SELECT 'every customer has a current row', COUNT(*) FROM silver_customers
  WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer WHERE is_current)
UNION ALL SELECT 'no overlapping customer versions', COUNT(*) FROM dim_customer a JOIN dim_customer b
  ON a.customer_id = b.customer_id AND a.customer_key < b.customer_key AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to
UNION ALL SELECT 'customer_key unique', COUNT(*) - COUNT(DISTINCT customer_key) FROM dim_customer
UNION ALL SELECT 'no null prices in fact', COUNT(*) FROM fact_sales WHERE unit_price IS NULL
UNION ALL SELECT 'no zero or negative quantity', COUNT(*) FROM fact_sales WHERE quantity <= 0
UNION ALL SELECT 'fact -> dim_customer integrity', COUNT(*) FROM fact_sales f LEFT JOIN dim_customer d USING (customer_key) WHERE d.customer_key IS NULL
UNION ALL SELECT 'fact customer version valid on order date', COUNT(*) FROM fact_sales f
  JOIN dim_customer d USING (customer_key) JOIN dim_date dd USING (date_key) WHERE dd.full_date NOT BETWEEN d.valid_from AND d.valid_to
UNION ALL SELECT 'fact -> dim_product integrity', COUNT(*) FROM fact_sales f LEFT JOIN dim_product d USING (product_key) WHERE d.product_key IS NULL
UNION ALL SELECT 'fact -> dim_date integrity', COUNT(*) FROM fact_sales f LEFT JOIN dim_date d USING (date_key) WHERE d.date_key IS NULL
UNION ALL SELECT 'every clean order is in fact', COUNT(*) FROM silver_orders WHERE order_id NOT IN (SELECT order_id FROM fact_sales)
UNION ALL SELECT 'fact lines match silver lines', ABS((SELECT COUNT(*) FROM fact_sales) - (SELECT COUNT(*) FROM silver_order_items))
UNION ALL SELECT 'cities are tidy', COUNT(*) FROM dim_customer WHERE city <> TRIM(city) OR city <> UPPER(LEFT(city,1)) || LOWER(SUBSTR(city,2));
