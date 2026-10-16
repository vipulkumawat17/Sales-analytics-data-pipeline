-- Query 08: Seller Coverage and Product Performance
-- Purpose: Compare products by seller coverage and sales performance.
-- Note: The product analytics table does not contain individual seller IDs.

SELECT
    product_id,
    product_category_name_english,
    distinct_sellers,
    units_sold,
    distinct_orders,
    ROUND(item_revenue, 2) AS item_revenue,
    ROUND(freight_cost, 2) AS freight_cost,

    RANK() OVER (
        ORDER BY distinct_sellers DESC NULLS LAST
    ) AS seller_coverage_rank,

    RANK() OVER (
        ORDER BY item_revenue DESC NULLS LAST
    ) AS revenue_rank,

    ROUND(
        1.0 * units_sold / NULLIF(distinct_orders, 0),
        2
    ) AS units_per_order

FROM public.product_analytics
ORDER BY
    seller_coverage_rank,
    revenue_rank
LIMIT 20;
