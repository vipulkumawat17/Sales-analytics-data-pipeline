-- Query 02: Order and Sales Performance by Order Status
-- Purpose: Compare order volume, revenue and delivery performance.
-- Source: public.order_analytics

SELECT
    order_status,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (
        WHERE is_delivered IS TRUE
    ) AS delivered_orders,
    ROUND(
        SUM(COALESCE(item_revenue, 0)),
        2
    ) AS total_item_revenue,
    ROUND(
        AVG(item_revenue),
        2
    ) AS average_revenue_per_order,
    ROUND(
        100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0),
        2
    ) AS order_volume_percentage
FROM public.order_analytics
GROUP BY order_status
ORDER BY total_orders DESC;
