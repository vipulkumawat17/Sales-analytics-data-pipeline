-- Query 01: Monthly Revenue and Growth Analysis
-- Purpose: Measure monthly item revenue and month-over-month growth.
-- Source: public.monthly_sales_analytics
-- Note: This is a recreated query; the original file was not recovered.

WITH monthly_metrics AS (
    SELECT
        order_month,
        order_count,
        item_revenue,
        freight_cost,
        total_items,
        orders_with_revenue,
        average_order_value,

        LAG(item_revenue) OVER (
            ORDER BY order_month
        ) AS previous_month_revenue

    FROM public.monthly_sales_analytics
)
SELECT
    order_month,
    order_count,
    ROUND(item_revenue::numeric, 2) AS item_revenue,
    ROUND(freight_cost::numeric, 2) AS freight_cost,
    total_items,
    orders_with_revenue,
    ROUND(average_order_value::numeric, 2) AS average_order_value,

    ROUND(
        previous_month_revenue::numeric,
        2
    ) AS previous_month_revenue,

    ROUND(
        (item_revenue - previous_month_revenue)::numeric,
        2
    ) AS revenue_change,

    ROUND(
        100.0 * (item_revenue - previous_month_revenue)
        / NULLIF(previous_month_revenue, 0),
        2
    ) AS revenue_growth_percentage

FROM monthly_metrics
ORDER BY order_month;
