-- Query 07: Top Customers by Revenue
-- Purpose: Rank customers by total item revenue.
-- Source: public.customer_analytics

SELECT
    customer_unique_id,
    customer_state,
    order_count,
    ROUND(item_revenue, 2) AS item_revenue,
    ROUND(average_order_value, 2) AS average_order_value,
    is_repeat_customer,

    RANK() OVER (
        ORDER BY item_revenue DESC NULLS LAST
    ) AS revenue_rank,

    ROUND(
        100.0 * item_revenue
        / NULLIF(SUM(item_revenue) OVER (), 0),
        4
    ) AS revenue_contribution_percentage

FROM public.customer_analytics
WHERE item_revenue IS NOT NULL

ORDER BY revenue_rank
LIMIT 20;
