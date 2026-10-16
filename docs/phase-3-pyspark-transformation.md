# Phase 3 — PySpark Transformation

## 1. Objective

Implement reusable, tested PySpark transformations for the Sales Analytics Data Engineering Pipeline. The Spark implementation processes the Olist e-commerce dataset and validates its analytics outputs against the existing Python/Pandas pipeline.

## 2. Environment

- Development platform: Google Colab
- Storage and version control: Google Drive and GitHub
- Processing framework: PySpark 4.0.4
- Java runtime: OpenJDK 21
- Spark execution mode: `local[*]`
- Shuffle partitions configured: 4
- Reference outputs: existing processed CSV analytics files
- Database integration from earlier phases: Neon PostgreSQL

## 3. Transformation implementation

Reusable transformations are implemented in `src/sales_pipeline/spark_transformations.py`.

### 3.1 Order analytics

`build_order_analytics_spark()` produces one row per order by independently aggregating order items and payments before joining them to orders and customer attributes.

Metrics include:

- Item count, unique products and unique sellers
- Item revenue and freight cost
- Payment total, payment record count and payment types
- Delivery duration, estimated duration and delivery delay
- Delivery and late-delivery flags

Count metrics are filled with zero when no corresponding item or payment records exist. Financial sums remain null when no source records exist, preserving the reference pipeline's behavior.

### 3.2 Eligible sales items

`prepare_sales_items_spark()` excludes orders with status `canceled` or `unavailable`, joins item records to product attributes and category translations, and assigns `unknown_or_untranslated` when an English category translation is unavailable.

### 3.3 Product analytics

`build_product_analytics_spark()` aggregates units sold, item revenue, freight cost, distinct orders and distinct sellers by product and English category.

### 3.4 Category analytics

`build_category_analytics_spark()` aggregates units sold, item revenue, freight cost, distinct orders, distinct products and distinct sellers by English category.

## 4. Data quality and reconciliation

The notebook performs schema-based reads, standardizes string values, and checks key integrity, duplicates, invalid numeric values and referential integrity.

Observed validation results:

| Dataset/check | Result |
|---|---:|
| Orders | 99,441 rows |
| Order items | 112,650 rows |
| Customers | 99,441 rows |
| Payment records | 103,886 rows |
| Products | 32,951 rows |
| Category translations | 71 rows |
| Eligible sales items | 112,101 rows |
| Order analytics output | 99,441 rows |
| Product analytics output | 32,729 rows |
| Category analytics output | 72 rows |

The observed source-data checks found no invalid order keys, duplicate order IDs, duplicate order-item keys, duplicate customer IDs, duplicate product IDs, duplicate category translation keys, orphan order-item/order links, or orphan order/customer links.

### 4.1 Reference reconciliation

Spark outputs were reconciled against the existing Pandas analytics outputs.

- Order analytics: 99,441 rows; all 13 shared metrics matched.
- Product analytics: 32,729 rows; no missing or extra products, category mismatches, or numerical mismatches.
- Category analytics: 72 categories; no missing or extra categories and no numerical mismatches.
- Product/category units and revenue totals reconciled.

Numeric revenue comparisons used a tolerance of 0.01 where applicable.

## 5. Partitioning experiment

The order analytics aggregation was measured using the baseline DataFrame and after repartitioning by `order_id` to four partitions.

| Configuration | Observed duration |
|---|---:|
| Baseline, 2 input partitions | 2.739 seconds |
| Repartitioned by order ID, 4 partitions | 2.838 seconds |

The aggregate metrics matched between the two runs. The single repartitioned run was approximately 3.6% slower. Based on this small, single-run experiment, no performance benefit was demonstrated, so the baseline configuration was retained.

These timings are exploratory observations, not a statistically reliable benchmark.

## 6. Automated tests

The reusable transformations are covered by `tests/test_spark_transformations.py`, which uses small in-memory Spark DataFrames.

The tests cover product/category aggregation, excluded order statuses, category translation fallback, order-level metric aggregation, one-row-per-order behavior, payment metrics and delivery flags.

The complete project test suite was run successfully:

- Total tests: 14
- Passed: 14
- Failed: 0
- Reported execution time: 54.389 seconds

The test run required installing the `psycopg[binary]` dependency in the test environment.

## 7. SQL analytics

Eight SQL analytics query files are maintained under `sql/`. Seven were restored from the referenced Git commit. `01_monthly_revenue_growth.sql` was recreated because the original could not be located in the available Git history or Google Drive.

The recreated monthly query calculates previous available month revenue, revenue change and month-over-month growth using `LAG()`. Because the monthly table contains gaps, the comparison is against the previous available row, which may not be the immediately preceding calendar month. The recreated query still requires execution against Neon to verify the target table schema and runtime behavior.

## 8. Files delivered in this phase

- `03_PySpark_transformation.ipynb`
- `src/sales_pipeline/spark_transformations.py`
- `tests/test_spark_transformations.py`
- `docs/phase-3-pyspark-transformation.md`
- SQL analytics files under `sql/`

## 9. Conclusion

Phase 3 implements reusable Spark transformations for order-, product- and category-level analytics. The Spark outputs reconciled with the existing Pandas results, and the complete 14-test project suite passed. The partition experiment did not establish a performance gain from increasing the partition count, so the baseline was retained. SQL execution validation and final Git review remain necessary before the phase is considered fully packaged.
