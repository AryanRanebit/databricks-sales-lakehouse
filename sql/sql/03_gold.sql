-- GOLD: star schema (Kimball). Grain of fact_sales: one row per order line.

-- ---- dim_product: Type 1 (overwrite). A price change just replaces the old price. ----
MERGE INTO dim_product t USING silver_products s ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET product_name = s.product_name, category = s.category, list_price = s.list_price
WHEN NOT MATCHED THEN INSERT VALUES (
  (SELECT COALESCE(MAX(product_key), 0) FROM dim_product) + s.product_id,  -- simple surrogate key
  s.product_id, s.product_name, s.category, s.list_price);

-- ---- dim_customer: Type 2 (keep history). ----
-- Step 1: customers whose city or name changed. Close their current row the day before the change.
UPDATE dim_customer d
SET valid_to = DATE '{as_of}' - INTERVAL 1 DAY, is_current = FALSE
FROM silver_customers s
WHERE d.customer_id = s.customer_id AND d.is_current
  AND (d.city <> s.city OR d.name <> s.name);

-- Step 2: insert a new current row for changed customers (no current row left) and brand new customers.
-- A brand new customer gets valid_from = 1900-01-01 so orders always find a row; a changed one starts at as_of.
INSERT INTO dim_customer
SELECT (SELECT COALESCE(MAX(customer_key), 0) FROM dim_customer) + ROW_NUMBER() OVER (ORDER BY s.customer_id),
       s.customer_id, s.name, s.city, s.signup_date,
       CASE WHEN EXISTS (SELECT 1 FROM dim_customer x WHERE x.customer_id = s.customer_id)
            THEN DATE '{as_of}' ELSE DATE '1900-01-01' END,
       DATE '9999-12-31', TRUE
FROM silver_customers s
WHERE NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id AND d.is_current);

-- ---- fact_sales: insert only lines for orders not in the fact yet ----
-- The customer_key is the version that was valid ON THE ORDER DATE. Old facts are never re-pointed.
INSERT INTO fact_sales
SELECT dc.customer_key, dp.product_key, CAST(strftime(o.order_date, '%Y%m%d') AS INTEGER),
       o.order_id, o.status, i.quantity, i.unit_price, i.quantity * i.unit_price
FROM silver_order_items i
JOIN silver_orders o USING (order_id)
JOIN dim_customer dc ON dc.customer_id = o.customer_id AND o.order_date BETWEEN dc.valid_from AND dc.valid_to
JOIN dim_product dp ON dp.product_id = i.product_id
WHERE o.order_id NOT IN (SELECT order_id FROM fact_sales);

-- Existing orders whose status changed: update the status only. customer_key is untouched.
UPDATE fact_sales f SET status = o.status FROM silver_orders o
WHERE f.order_id = o.order_id AND f.status <> o.status;
