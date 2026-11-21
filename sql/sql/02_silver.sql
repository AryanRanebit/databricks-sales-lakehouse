-- SILVER: clean this batch, then MERGE (upsert) it into the silver tables.
-- MERGE = "update the row if the key exists, insert it if not". Delta Lake on Databricks has the same statement.

CREATE OR REPLACE TEMP TABLE stg_customers AS
SELECT DISTINCT
  CAST(customer_id AS INTEGER) AS customer_id,
  TRIM(name) AS name,
  UPPER(LEFT(TRIM(city), 1)) || LOWER(SUBSTR(TRIM(city), 2)) AS city,
  CAST(signup_date AS DATE) AS signup_date
FROM bronze_customers WHERE batch_id = '{batch}';

CREATE OR REPLACE TEMP TABLE stg_products AS
SELECT CAST(product_id AS INTEGER) AS product_id, TRIM(product_name) AS product_name, category,
       CAST(list_price AS DECIMAL(10,2)) AS list_price
FROM bronze_products WHERE batch_id = '{batch}';

CREATE OR REPLACE TEMP TABLE stg_orders AS
SELECT CAST(order_id AS INTEGER) AS order_id, CAST(customer_id AS INTEGER) AS customer_id,
       CAST(order_date AS DATE) AS order_date, status
FROM bronze_orders WHERE batch_id = '{batch}';

-- Rule: a missing unit price is filled from the product's list price.
CREATE OR REPLACE TEMP TABLE stg_order_items AS
SELECT CAST(i.order_id AS INTEGER) AS order_id, CAST(i.product_id AS INTEGER) AS product_id,
       CAST(i.quantity AS INTEGER) AS quantity,
       COALESCE(NULLIF(i.unit_price, ''), p.list_price)::DECIMAL(10,2) AS unit_price
FROM bronze_order_items i
LEFT JOIN (SELECT DISTINCT product_id, list_price FROM bronze_products WHERE batch_id = '{batch}') p USING (product_id)
WHERE i.batch_id = '{batch}';

MERGE INTO silver_customers t USING stg_customers s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET name = s.name, city = s.city, signup_date = s.signup_date
WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, s.name, s.city, s.signup_date);

MERGE INTO silver_products t USING stg_products s ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET product_name = s.product_name, category = s.category, list_price = s.list_price
WHEN NOT MATCHED THEN INSERT VALUES (s.product_id, s.product_name, s.category, s.list_price);

-- Rejects: orders whose customer does not exist (checked against ALL customers loaded so far).
-- Kept in their own table, not deleted, so they can be investigated.
CREATE OR REPLACE TEMP TABLE stg_rejected AS
SELECT o.*, 'unknown customer' AS reason, '{batch}' AS batch_id
FROM stg_orders o LEFT JOIN silver_customers c USING (customer_id)
WHERE c.customer_id IS NULL;

MERGE INTO silver_rejected_orders t USING stg_rejected s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET status = s.status
WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.order_date, s.status, s.reason, s.batch_id);

-- Good orders: new ones are inserted, existing ones get the new status.
MERGE INTO silver_orders t USING (SELECT * FROM stg_orders WHERE order_id NOT IN (SELECT order_id FROM stg_rejected)) s
  ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET status = s.status
WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.order_date, s.status);

-- Items only arrive with NEW orders. Insert lines for orders we have not stored items for yet.
INSERT INTO silver_order_items
SELECT * FROM stg_order_items
WHERE order_id IN (SELECT order_id FROM silver_orders)
  AND order_id NOT IN (SELECT order_id FROM silver_order_items);
