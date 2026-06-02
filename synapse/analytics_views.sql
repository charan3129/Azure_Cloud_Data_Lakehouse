-- ============================================================
-- Synapse Analytics Views for BI Consumption
-- ============================================================

-- Revenue by state with running totals
CREATE OR ALTER VIEW vw_revenue_by_state AS
SELECT
    order_day,
    address_state,
    SUM(net_revenue) AS daily_net_revenue,
    SUM(order_count) AS daily_orders,
    SUM(unique_customers) AS daily_unique_customers,
    SUM(SUM(net_revenue)) OVER (
        PARTITION BY address_state
        ORDER BY order_day
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_revenue
FROM ext_gold_daily_sales_agg
GROUP BY order_day, address_state;

-- Customer value distribution
CREATE OR ALTER VIEW vw_customer_value_distribution AS
SELECT
    value_segment,
    loyalty_tier,
    COUNT(*) AS customer_count,
    AVG(lifetime_revenue) AS avg_lifetime_revenue,
    AVG(avg_order_value) AS avg_order_value,
    AVG(lifetime_orders) AS avg_order_count,
    AVG(total_sessions) AS avg_sessions,
    SUM(lifetime_revenue) AS segment_total_revenue
FROM ext_gold_customer_360
GROUP BY value_segment, loyalty_tier;

-- Inventory health dashboard
CREATE OR ALTER VIEW vw_inventory_health AS
SELECT
    stock_status,
    warehouse_location,
    COUNT(*) AS product_count,
    SUM(inventory_value) AS total_inventory_value,
    AVG(quantity_on_hand) AS avg_quantity,
    SUM(CASE WHEN stock_status = 'REORDER_NOW' THEN 1 ELSE 0 END)
        AS reorder_alert_count
FROM ext_gold_inventory_snapshot
GROUP BY stock_status, warehouse_location;

-- Payment method analysis
CREATE OR ALTER VIEW vw_payment_analysis AS
SELECT
    payment_method,
    customer_segment,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    AVG(discount_amount) AS avg_discount
FROM ext_gold_orders_wide
GROUP BY payment_method, customer_segment;

-- Top customers by revenue
CREATE OR ALTER VIEW vw_top_customers AS
SELECT TOP 100
    customer_id,
    first_name,
    last_name,
    address_state,
    loyalty_tier,
    value_segment,
    lifetime_orders,
    lifetime_revenue,
    avg_order_value,
    total_sessions,
    first_order_date,
    last_order_date
FROM ext_gold_customer_360
ORDER BY lifetime_revenue DESC;
