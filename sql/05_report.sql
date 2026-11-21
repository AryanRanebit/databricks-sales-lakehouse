-- Monthly revenue by customer city as it was at order time (delivered orders only).
SELECT d.month, c.city, ROUND(SUM(f.line_amount), 2) AS revenue
FROM fact_sales f
JOIN dim_date d USING (date_key)
JOIN dim_customer c USING (customer_key)
WHERE f.status = 'delivered'
GROUP BY d.month, c.city
ORDER BY d.month, revenue DESC;
