# Phase 7 — Cloud Deployment Extension

## 1. Architecture

| Component | Responsibility |
|---|---|
| Google Colab | Development and pipeline orchestration |
| Cloudflare R2 | Bronze, Silver and Gold Parquet storage |
| PySpark | Data transformation and analytics |
| Neon PostgreSQL | Existing analytical warehouse |
| GitHub | Source code and version control |

## 2. Data Flow

1. Source datasets are stored in the Bronze layer.
2. Silver and Gold Parquet datasets are stored in Cloudflare R2.
3. PySpark reads six Silver datasets from R2.
4. Transformations generate order, product and category analytics.
5. Gold outputs are uploaded to dedicated R2 prefixes.
6. Uploaded data is independently read back and reconciled.

## 3. Security and Access

- R2 access uses bucket-scoped Object Read and Write credentials.
- Credentials are stored in Google Colab Secrets.
- Secret values are never stored in source code or GitHub.
- Existing R2 access permissions are restricted to the project bucket.
- Neon PostgreSQL credentials remain separately managed.

## 4. Reliability and Validation

- SHA256 checksum verification for uploaded objects.
- Existing-object protection using error mode.
- Multi-file Parquet read and write support.
- Spark output reconciliation against existing Gold data.
- Independent R2 read-back verification.
- Automated failure-handling tests.

## 5. Implementation Evidence

- Six Silver datasets successfully read from R2.
- Three Gold datasets generated using PySpark.
- 132,242 Gold rows independently reconciled.
- Five Parquet objects uploaded in the orchestrator test.
- Eight cloud orchestration tests passed.
- Project-wide regression suite: 63 tests, zero failures and errors.
- One database integration test skipped because NEON_DB_URL was unavailable.

## 6. Limitations

- PySpark execution is currently performed in Google Colab, not a managed Spark cluster.
- Cloudflare R2 provides object storage, not managed ETL orchestration.
- The cloud orchestrator currently generates three Gold analytics datasets.
- Neon PostgreSQL remains the existing warehouse; the new cloud orchestrator does not load its outputs into Neon.
- Continuous scheduling and production monitoring are not implemented.

## 7. Cost Considerations

- No new paid compute service was introduced for this implementation.
- Colab and R2 usage remain subject to their respective account limits and billing policies.
- Monitor storage, operations, and any applicable account charges.
- No additional paid infrastructure is required by the current implementation.

## 8. Conclusion

Phase 7 successfully extends the Sales Analytics Data Engineering Pipeline
with Cloudflare R2 storage, PySpark integration, reusable cloud orchestration,
and independently verified Gold analytics outputs.
