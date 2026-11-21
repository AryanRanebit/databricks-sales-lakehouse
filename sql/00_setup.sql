-- Run once. Tables that keep state between batches.
-- bronze_* keep every batch we ever loaded (batch_id says which one).
CREATE TABLE IF NOT EXISTS bronze_batches (batch_id VARCHAR, as_of DATE, loaded_at TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS bronze_customers   (customer_id VARCHAR, name VARCHAR, city VARCHAR, signup_date VARCHAR, batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS bronze_products    (product_id VARCHAR, product_name VARCHAR, category VARCHAR, list_price VARCHAR, batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS bronze_orders      (order_id VARCHAR, customer_id VARCHAR, order_date VARCHAR, status VARCHAR, batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS bronze_order_items (order_id VARCHAR, product_id VARCHAR, quantity VARCHAR, unit_price VARCHAR, batch_id VARCHAR);

CREATE TABLE IF NOT EXISTS silver_customers   (customer_id INTEGER, name VARCHAR, city VARCHAR, signup_date DATE);
CREATE TABLE IF NOT EXISTS silver_products    (product_id INTEGER, product_name VARCHAR, category VARCHAR, list_price DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS silver_orders      (order_id INTEGER, customer_id INTEGER, order_date DATE, status VARCHAR);
CREATE TABLE IF NOT EXISTS silver_order_items (order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS silver_rejected_orders (order_id INTEGER, customer_id INTEGER, order_date DATE, status VARCHAR, reason VARCHAR, batch_id VARCHAR);

-- Gold. dim_customer is SCD Type 2: one row per VERSION of a customer.
CREATE TABLE IF NOT EXISTS dim_customer (customer_key INTEGER, customer_id INTEGER, name VARCHAR, city VARCHAR,
                                         signup_date DATE, valid_from DATE, valid_to DATE, is_current BOOLEAN);
CREATE TABLE IF NOT EXISTS dim_product  (product_key INTEGER, product_id INTEGER, product_name VARCHAR, category VARCHAR, list_price DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS dim_date AS
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key, d AS full_date, YEAR(d) AS year, MONTH(d) AS month,
       strftime(d, '%B') AS month_name, DAYOFWEEK(d) AS day_of_week
FROM (SELECT UNNEST(generate_series(DATE '2026-01-01', DATE '2027-12-31', INTERVAL 1 DAY))::DATE AS d);
CREATE TABLE IF NOT EXISTS fact_sales (customer_key INTEGER, product_key INTEGER, date_key INTEGER, order_id INTEGER,
                                       status VARCHAR, quantity INTEGER, unit_price DECIMAL(10,2), line_amount DECIMAL(12,2));
