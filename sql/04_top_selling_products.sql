-- Query 04: Top-Selling Products
-- Purpose: Rank products by units sold and item revenue.
-- Source: public.product_analytics

WITH ranked_products AS (
    SELECT
        product_id,
        product_category_name_english,
        units_sold,
        item_revenue,
        distinct_orders,
        distinct_sellers,

        RANK() OVER (
            ORDER BY units_sold DESC NULLS LAST
        ) AS units_sold_rank,

        RANK() OVER (
            ORDER BY item_revenue DESC NULLS LAST
        ) AS revenue_rank,

        ROUND(
            100.0 * item_revenue
            / NULLIF(SUM(item_revenue) OVER (), 0),
            2
        ) AS revenue_contribution_percentage

    FROM public.product_analytics
)
SELECT
    product_id,
    product_category_name_english,
    units_sold,
    ROUND(item_revenue, 2) AS item_revenue,
    distinct_orders,
    distinct_sellers,
    units_sold_rank,
    revenue_rank,
    revenue_contribution_percentage
FROM ranked_products
WHERE units_sold_rank <= 10
ORDER BY units_sold_rank, item_revenue DESC;
