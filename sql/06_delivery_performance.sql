-- Query 06: Delivery Performance Analysis
-- Purpose: Measure delivery completion, delays, and duration.
-- Source: public.order_analytics

SELECT
    COUNT(*) AS total_orders,

    COUNT(*) FILTER (
        WHERE is_delivered IS TRUE
    ) AS delivered_orders,

    COUNT(*) FILTER (
        WHERE is_delivered IS FALSE
           OR is_delivered IS NULL
    ) AS not_delivered_orders,

    COUNT(*) FILTER (
        WHERE is_late_delivery IS TRUE
    ) AS late_delivery_orders,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE is_delivered IS TRUE
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS delivery_rate_percentage,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE is_late_delivery IS TRUE
        ) / NULLIF(
            COUNT(*) FILTER (
                WHERE is_delivered IS TRUE
            ),
            0
        ),
        2
    ) AS late_delivery_percentage_of_all_orders_delivered,

    ROUND(
        AVG(delivery_duration_days) FILTER (
            WHERE is_delivered IS TRUE
        )::numeric,
        2
    ) AS average_delivery_days,

    ROUND(
        AVG(delivery_delay_days) FILTER (
            WHERE is_delivered IS TRUE
        )::numeric,
        2
    ) AS average_delivery_delay_days

FROM public.order_analytics;
