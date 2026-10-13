# RetailRecon — Merchandise Reconciliation Pipeline

A retail finance reconciliation pipeline that matches cancellation transactions
to prior sales using FIFO allocation, built on Databricks Serverless with PySpark.

## Overview

RetailRecon processes the [UCI Online Retail II dataset](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
through five stages: source extraction, type casting and overlap exclusion,
transaction classification, FIFO cancellation allocation, and financial
reporting. All financial calculations use `Decimal` arithmetic for exact
reconciliation. The pipeline is fully deterministic and idempotent.

### Pipeline Stages

| Stage | Description | Output |
| --- | --- | --- |
| 1–2 | XLSX extraction, type casting, overlap exclusion | `source_raw_parquet`, `source_typed_parquet`, `source_overlap_exclusions_parquet` |
| 3 | Transaction classification (SALE / CANCELLATION / ADJUSTMENT / EXCEPTION) | `classified_sales_ledger`, `classified_adjustments`, `classified_exceptions` |
| 4 | FIFO allocation via `applyInPandas` with deterministic SHA-256 IDs | `cancellation_allocations_parquet`, `cancellation_status_parquet` |
| 5 | Financial reporting: invoice, monthly-country, customer history | `invoice_summary_parquet`, `monthly_country_sales_parquet`, `customer_purchase_history_parquet` |

### Key Results

| Metric | Value |
| --- | --- |
| Raw source rows | 1,067,371 |
| Merchandise ledger rows | 1,033,442 |
| FIFO allocations | 16,339 |
| Allocated units | 433,303 |
| Gross sales | GBP 19,713,221.70 |
| Cancellation value | GBP 771,920.31 |
| Net sales | GBP 18,941,301.39 |
| Validation tests passed | 68 / 68 |

### Performance Benchmark

The baseline FIFO allocation processes all 501,527 matching groups through
`applyInPandas`. An optimized version pre-filters to the 15,439 groups
containing at least one cancellation, achieving a **26x speedup**
(1,046s to 40s) with identical output verified by deterministic fingerprints.

## Repository Structure

```
retail-recon-databricks/
├── 01_environment_check.ipynb    # Full pipeline (21 cells, phases 1–7)
├── docs/
│   └── PHASE6_READINESS_REPORT.md # Architecture, schemas, validation results
├── databricks.yml                # DABs config (dev target only)
├── README.md
└── .gitignore
```

## Setup

### Prerequisites

- Databricks workspace with Serverless compute
- Unity Catalog volume `portfolio_projects.retail_sales.raw` containing
  the source workbook `online_retail_II.xlsx` and all intermediate Parquet outputs

### Running the Pipeline

1. Open `01_environment_check` in the Databricks workspace
2. Attach to Serverless compute
3. Run cells in order (1–21):
   - Cells 1–8: Source extraction and typing
   - Cell 9: Transaction classification
   - Cell 10: FIFO allocation
   - Cells 11–12: Synthetic tests and post-write validation
   - Cells 13–16: Financial reporting and reconciliation tests
   - Cells 17–19: Phase 6 quality gates, idempotency, benchmark
   - Cells 20–21: Phase 7 final validation and idempotency

### Declarative Automation Bundle

This project uses a Declarative Automation Bundle for development deployment.
See the [DABs documentation](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
for configuration details.

## Data Attribution

The source dataset is the **UCI Online Retail II** dataset by Chen et al.
It is licensed separately from this code and is not included in this repository.
See the [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
for dataset details and licensing terms.

## Status

**Development-validated.** Not production-ready. No production resources,
schedules, or jobs have been created.

## Limitations

- Runs on development Serverless compute only
- Single-notebook architecture (refactoring to modular package is planned)
- Spark shuffle/spill metrics not available on Serverless Spark Connect
- No incremental or streaming data ingestion configured
