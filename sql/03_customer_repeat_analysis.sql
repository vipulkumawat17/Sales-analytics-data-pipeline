-- Query 03: Repeat Customer and Spending Analysis
-- Purpose: Measure customer retention and compare spending.
-- Source: public.customer_analytics

SELECT
    CASE
        WHEN is_repeat_customer IS TRUE THEN 'Repeat customer'
        WHEN is_repeat_customer IS FALSE THEN 'One-time customer'
        ELSE 'Unknown'
    END AS customer_segment,

    COUNT(*) AS customer_count,

    SUM(order_count) AS total_orders,

    ROUND(
        SUM(COALESCE(item_revenue, 0)),
        2
    ) AS total_item_revenue,

    ROUND(
        AVG(average_order_value),
        2
    ) AS average_customer_order_value,

    ROUND(
        100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0),
        2
    ) AS customer_percentage

FROM public.customer_analytics
GROUP BY
    CASE
        WHEN is_repeat_customer IS TRUE THEN 'Repeat customer'
        WHEN is_repeat_customer IS FALSE THEN 'One-time customer'
        ELSE 'Unknown'
    END
ORDER BY total_item_revenue DESC;
