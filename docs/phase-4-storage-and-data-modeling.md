# Phase 4 — Storage and Data Modeling

## 1. Objective

Phase 4 establishes persistent Bronze, Silver, and Gold data layers using Parquet and introduces a dimensional model for sales analytics. The goal is to make processed datasets reloadable, queryable, and reconcilable against established analytics outputs.

## 2. Architecture

- **Bronze:** Raw Olist source datasets persisted as Parquet snapshots.
- **Silver:** Structured source datasets persisted as Parquet, with data-quality and relationship checks.
- **Gold analytics:** Existing Phase 1 analytics outputs persisted as Parquet.
- **Gold dimensional:** Fact and dimension tables designed for analytical queries.
- **PostgreSQL:** Existing Phase 2 relational warehouse retained separately; Phase 4 does not replace its tables.

All persisted datasets reside under the project's `data/` directory. Data files are excluded from Git by the existing `.gitignore` policy.

## 3. Persisted dataset inventory

### Bronze source snapshots

| Dataset | Rows |
|---|---:|
| customers | 99,441 |
| orders | 99,441 |
| order_items | 112,650 |
| payments | 103,886 |
| reviews | 104,162 in the original Spark parse |
| geolocation | 1,000,163 |
| products | 32,951 |
| sellers | 3,095 |
| category_translation | 71 |

### Gold analytics tables

| Table | Grain | Rows |
|---|---|---:|
| order_analytics | One row per order | 99,441 |
| product_analytics | Product/category aggregate | 32,729 |
| category_analytics | Product category aggregate | 72 |
| customer_analytics | Customer identity aggregate | 94,990 |
| monthly_sales_analytics | One row per month | 24 |
| order_status_summary | One row per order status | 8 |

### Gold dimensional tables

| Table | Grain | Rows |
|---|---|---:|
| dim_customer | One row per customer_id | 99,441 |
| dim_product | One row per product_id | 32,951 |
| dim_seller | One row per seller_id | 3,095 |
| dim_order | One row per order_id | 99,441 |
| fact_sales | One row per order item | 112,650 |

The fact table retains all order statuses. Reconciliation to the Phase 1 sales analytics excludes `canceled` and `unavailable` orders according to the established sales transformation logic.

## 4. Fact and dimension design

### fact_sales

The fact table contains the order-item grain, including `order_id`, `order_item_id`, dimension surrogate keys, `price`, `freight_value`, and shipping-limit date.

The composite business key is `(order_id, order_item_id)`. Revenue and freight are measured at the order-item grain.

### Dimensions

- `dim_customer` uses `customer_id` as its business key and retains `customer_unique_id` as an identity attribute. This avoids silently merging source customer records that share a `customer_unique_id`.
- `dim_product` uses `product_id`.
- `dim_seller` uses `seller_id`.
- `dim_order` uses `order_id`.

Surrogate keys are deterministic for a fixed source snapshot, but may change if new business keys are inserted earlier in the ordering. They should not be treated as permanently stable identifiers across independently rebuilt snapshots.

## 5. Validation evidence

### Silver SQL integrity

Fifteen SQL integrity checks passed, covering required keys, duplicate business keys, order-item composite-key uniqueness, non-negative item and payment values, category translation uniqueness, and review-candidate constraints.

### Silver relationships

The following relationships passed with zero unmatched records:

- orders → customers
- order_items → orders
- order_items → products
- order_items → sellers
- payments → orders

The original reviews dataset has a documented parsing exception described below. The separate multiline-parsed review candidate passed the tested composite-key, score-range, and order-reference checks.

### Gold SQL validation

The previously executed Gold SQL validation passed all 10 checks, including fact-table grain, required keys, dimension references, and sales metric reconciliation.

The eligible fact dataset contains 112,101 order items. Revenue and freight reconcile with Phase 1 monthly sales analytics within a tolerance of 0.01.

### Automated tests

The existing test suite passed 14 tests after the test environment was configured with the declared PostgreSQL driver and the repository `src` directory on `PYTHONPATH`.

The new Phase 4 test file passed 6 tests covering persisted Gold row counts, dimension keys, fact grain, fact-to-dimension references, and monthly sales reconciliation.

## 6. Known exception: reviews CSV parsing

The original persisted Silver reviews dataset contains 104,162 rows and produces 4,938 unmatched order references in the current relationship check.

An independent multiline CSV parse produces 99,224 logical records with seven columns, no null review/order IDs, valid scores from 1 to 5, no duplicate `(review_id, order_id)` pairs, and zero unmatched order references.

The raw-source audit found repeated `review_id` values, so `review_id` alone is not a valid uniqueness constraint for this source. No exact duplicate full rows were found in the 99,224-record candidate.

The 4,938-row discrepancy between the original Spark parse and the multiline candidate remains documented and unresolved. The candidate is retained separately; the original persisted Silver dataset and ingestion modules were not overwritten or changed.

## 7. Scope and limitations

- Existing Phase 1, Phase 2, and Phase 3 modules and outputs were preserved.
- Existing PostgreSQL warehouse tables were not replaced.
- The reviews parsing discrepancy remains a documented exception rather than being silently repaired.
- Phase 4 has not yet been committed or pushed to Git.
- Final full-suite verification and Git review are required before considering Phase 4 ready for delivery.

## 8. Acceptance summary

| Requirement | Status |
|---|---|
| Bronze, Silver, and Gold layers persisted | Implemented |
| Parquet outputs reload successfully | Validated |
| Sales fact and dimensions implemented | Validated |
| Gold SQL reconciliation | 10 checks passed |
| Silver SQL integrity | 15 checks passed |
| Phase 4 automated tests | 6 tests passed |
| Reviews parsing discrepancy documented | Yes; unresolved |
| Final full-suite run and Git review | Pending |
