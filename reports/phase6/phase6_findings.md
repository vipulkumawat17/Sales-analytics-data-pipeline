# Phase 6 — Analytics and Performance Evaluation

## 1. Evaluation scope

This phase evaluates sales trends, product and customer rankings, data quality, and transformation performance using the Olist Brazilian E-Commerce dataset.

## 2. Sales analytics

- Daily sales records: 634
- Monthly sales records: 24
- Orders represented in daily metrics: 99,441
- Monthly metrics were reproduced using the existing sales transformation rules and reconciled against the saved monthly reference across all 24 months.

## 3. Product and customer analysis

- Products ranked: 32,729
- Customers ranked: 94,990
- Highest-revenue product: `bb50f2e236e5eea0100680137654686c`
- Its item revenue: 63,885.00
- Highest-revenue customer: `0a0a92112bd4c708ca5fde585afaa872`
- Their item revenue: 13,440.00

Rankings use deterministic ordering and the existing analytics datasets. They should be interpreted within the populations and transformation rules represented by those datasets.

## 4. Data quality evaluation

- Validation checks passed: 27 of 27
- Validation check pass rate: 100.00%
- Required-field null rate: 0.0000%
- Duplicate-key rate: 0.0000%
- Referential-integrity violation rate: 0.0000%
- Invalid numeric-value rate: 0.0000%
- Invalid review-score rate: 0.0000%

All 27 existing validation checks passed.

## 5. Performance evaluation

The benchmark compared equivalent order-item aggregation by `order_id` using the same 112,650 input records.

- Pandas median execution time: 0.271482 seconds
- PySpark median execution time: 0.881575 seconds
- PySpark slowdown relative to Pandas: 3.25x
- Both implementations produced 98,666 output rows.
- Item count, item revenue, and freight cost reconciled successfully.

For this dataset and local execution environment, Pandas was faster. This benchmark measures transformation execution only; it does not measure distributed-cluster performance or end-to-end pipeline duration.

## 6. Limitations and bottlenecks

- The benchmark was conducted on a single local environment using `local[*]`, not on a distributed Spark cluster.
- The benchmark covers one equivalent aggregation workload, not every transformation in the pipeline.
- Spark startup, scheduling, and execution overhead can outweigh parallel-processing benefits for relatively small workloads.
- Performance results may differ with larger datasets, different hardware, memory settings, partitioning, and cluster configuration.
- Rankings and sales metrics inherit the filtering and aggregation rules of their source analytics datasets.
- Execution times are based on three measured runs after warm-up; they are indicative rather than a comprehensive performance study.

## 7. Conclusion

Phase 6 produced daily and monthly sales metrics, product and customer rankings, data-quality evaluation metrics, and a reconciled Pandas-versus-PySpark benchmark. All 27 data-quality checks passed, monthly metrics reconciled across 24 months, and the benchmark outputs matched.

The evaluation demonstrates reproducible analytics and validation for the current dataset. Larger-scale distributed benchmarking would be required before drawing conclusions about engine performance at production scale.
