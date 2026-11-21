-- BRONZE: append the raw batch as it arrived (all text, nothing cleaned).
-- Python skips this whole batch if batch_id is already in bronze_batches, so re-running is safe.
INSERT INTO bronze_customers   SELECT *, '{batch}' FROM read_csv('{raw}/customers.csv',   all_varchar=true);
INSERT INTO bronze_products    SELECT *, '{batch}' FROM read_csv('{raw}/products.csv',    all_varchar=true);
INSERT INTO bronze_orders      SELECT *, '{batch}' FROM read_csv('{raw}/orders.csv',      all_varchar=true);
INSERT INTO bronze_order_items SELECT *, '{batch}' FROM read_csv('{raw}/order_items.csv', all_varchar=true);
