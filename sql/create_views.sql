-- Gold layer SQL views for the analytics warehouse
-- Deployed automatically by fab-bundle

CREATE OR ALTER VIEW vw_daily_revenue AS
SELECT
    order_date,
    total_orders,
    revenue,
    avg_order_value,
    unique_customers,
    unique_products,
    revenue / NULLIF(total_orders, 0) AS revenue_per_order
FROM gold.daily_revenue;

CREATE OR ALTER VIEW vw_customer_ltv AS
SELECT
    customer_id,
    lifetime_orders,
    lifetime_value,
    avg_order_value,
    first_order,
    last_order,
    days_active,
    lifetime_value / NULLIF(lifetime_orders, 0) AS value_per_order
FROM gold.customer_ltv;

CREATE OR ALTER VIEW vw_product_performance AS
SELECT
    product_id,
    total_orders,
    total_revenue,
    unique_buyers,
    total_revenue / NULLIF(total_orders, 0) AS avg_revenue_per_order
FROM gold.product_performance
ORDER BY total_revenue DESC;

CREATE OR ALTER VIEW vw_executive_summary AS
SELECT
    COUNT(DISTINCT order_date) AS active_days,
    SUM(total_orders) AS total_orders,
    SUM(revenue) AS total_revenue,
    AVG(avg_order_value) AS avg_order_value,
    SUM(unique_customers) AS total_customer_visits
FROM gold.daily_revenue;
