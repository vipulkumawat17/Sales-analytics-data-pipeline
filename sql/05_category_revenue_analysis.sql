-- Query 05: Category-wise Revenue Analysis
-- Purpose: Rank categories by revenue and measure contribution.
-- Source: public.category_analytics

WITH category_metrics AS (
    SELECT
        product_category_name_english,
        units_sold,
        item_revenue,
        freight_cost,
        distinct_orders,
        distinct_products,
        distinct_sellers,

        RANK() OVER (
            ORDER BY item_revenue DESC NULLS LAST
        ) AS revenue_rank,

        ROUND(
            100.0 * item_revenue
            / NULLIF(SUM(item_revenue) OVER (), 0),
            2
        ) AS revenue_contribution_percentage

    FROM public.category_analytics
)
SELECT
    product_category_name_english,
    units_sold,
    ROUND(item_revenue, 2) AS item_revenue,
    ROUND(freight_cost, 2) AS freight_cost,
    distinct_orders,
    distinct_products,
    distinct_sellers,
    revenue_rank,
    revenue_contribution_percentage
FROM category_metrics
ORDER BY revenue_rank, product_category_name_english;
