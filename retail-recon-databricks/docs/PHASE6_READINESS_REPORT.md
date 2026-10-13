# RetailRecon — Phase 6 Final Readiness Report

**Status**: Development-validated. Not production-ready.
**Date**: 2026-10-12
**Compute**: Databricks Serverless (GPU-backed A10, development only)
**Notebook**: `01_environment_check` (ID: 2863572223322685)
**Project path**: `/Workspace/Users/jagadeeswaran.vp@sstcloud.com.au/RetailRecon/retail-recon-databricks`

---

## 1. Architecture and Processing Stages

The RetailRecon pipeline is implemented as a single multi-cell notebook (`01_environment_check`) executing on Databricks Serverless compute. The pipeline stages are:

| Phase | Cells | Description |
| --- | --- | --- |
| 1–2 | 1–8 | Environment check, source workbook extraction, type casting, overlap exclusion |
| 3 | 9 | Transaction classification: SALE, CANCELLATION, ADJUSTMENT, EXCEPTION |
| 4 | 10 | FIFO cancellation allocation via `applyInPandas` with staging Parquet |
| 4 | 11 | Synthetic FIFO correctness tests (6 scenarios A–F) |
| 4 | 12 | Post-write validation: counts, over-allocation, timestamp ordering, allocation ID uniqueness |
| 5 | 13–14 | Financial reporting: invoice summary, monthly-country sales |
| 5 | 15 | Customer purchase history with cumulative lifetime values |
| 5 | 16 | Financial reconciliation tests A–F (6 tests) |
| 6 | 17 | End-to-end row-accounting checks (14 checks) |
| 6 | 18 | Idempotency test: FIFO rerun + fingerprint comparison |
| 6 | 19 | Performance benchmark: baseline vs pre-filtered optimization |

### Data Flow

```
Source XLSX → Raw Parquet → Typed Parquet (with overlap exclusions)
  → Classified Ledger (SALE / CANCELLATION / ADJUSTMENT / EXCEPTION)
    → FIFO Allocation (applyInPandas per customer+stock+price group)
      → Cancellation Allocations + Cancellation Status
    → Invoice Summary + Monthly-Country Summary + Customer Purchase History
```

---

## 2. Dataset Row Counts and Schemas

All datasets are persisted as Parquet in the Unity Catalog volume `/Volumes/portfolio_projects/retail_sales/raw/`.

| Dataset | Path | Row Count | Key Columns |
| --- | --- | --- | --- |
| Raw source | `source_raw_parquet` | 1,067,371 | row_id (string) |
| Typed source | `source_typed_parquet` | 1,044,848 | row_id (string) |
| Overlap exclusions | `source_overlap_exclusions_parquet` | 22,523 | row_id (string) |
| Classified ledger | `classified_sales_ledger` | 1,033,442 | row_id (string), transaction_type |
| Adjustments | `classified_adjustments` | 5,377 | row_id (string) |
| Exceptions | `classified_exceptions` | 6,029 | row_id (string) |
| FIFO allocations | `cancellation_allocations_parquet` | 16,339 | allocation_id (string) |
| Cancellation status | `cancellation_status_parquet` | 18,191 | cancellation_row_id (string) |
| Invoice summary | `invoice_summary_parquet` | 47,155 | invoice (string) |
| Monthly-country | `monthly_country_sales_parquet` | 585 | invoice_year, invoice_month, country |
| Customer history | `customer_purchase_history_parquet` | 36,669 | customer_id, purchase_invoice |

### Key Schema Notes

* **Classified ledger** (23 fields): `invoice`, `stock_code`, `description`, `quantity` (integer), `invoice_date` (timestamp), `price` (decimal(18,4)), `customer_id`, `country`, `row_id`, `transaction_type`, `signed_line_value` (decimal(30,4)), `invoice_year`, `invoice_month`, plus raw variants and `exclusion_reason`.
* **FIFO allocations** (14 fields): `allocation_id` (SHA-256 hash of `sale_row_id||cancellation_row_id`), `cancellation_row_id`, `sale_row_id`, `allocated_quantity` (long), `allocation_value` (decimal(30,4)), `sale_date`, `cancellation_date`, `days_to_cancellation` (int).
* **Cancellation status** (12 fields): `cancellation_row_id`, `cancellation_status` (MATCHED/PARTIALLY_MATCHED/UNMATCHED), `unmatched_reason`, `requested_quantity`, `matched_quantity`, `unmatched_quantity`.
* **Invoice summary** (10 fields): `invoice`, `invoice_date`, `country`, `gross_sales_value` (decimal(30,4)), `cancellation_value` (decimal(30,4)), `net_sales_value` (decimal(30,4)), line/quantity counts.
* **Customer history** (10 fields): `customer_id`, `purchase_invoice`, `purchase_invoice_timestamp`, `purchase_country`, `invoice_gross_value`, `lifetime_gross_sales_value`, `lifetime_cancellation_value`, `first_purchase_date`, `previous_purchase_invoice_date`, `days_between_purchases`.

### Financial Totals (Verified)

| Metric | Value |
| --- | --- |
| Gross sales value | GBP 19,713,221.70 |
| Cancellation value | GBP 771,920.31 |
| Net sales value | GBP 18,941,301.39 |
| Allocated (matched) quantity | 433,303 units |
| Unmatched cancellation quantity | 36,807 units |
| Total cancelled quantity | 470,110 units |
| Distinct identified customers | 5,861 |

---

## 3. Source Lineage and Overlap Strategy

The source data is an Excel workbook (`online_retail_II.xlsx`) containing two sheets of retail transaction records.

1. **Extraction**: All rows from both sheets are extracted to `source_raw_parquet` with a computed `row_id` (SHA-256 of source sheet + row number + workbook hash) for unique identification.
2. **Overlap exclusion**: Rows appearing in both sheets (detected by matching invoice, stock_code, quantity, invoice_date, price, customer_id) are flagged and excluded. Duplicate rows are written to `source_overlap_exclusions_parquet` (22,523 rows).
3. **Type casting**: Retained rows are typed (string → integer/timestamp/decimal) and written to `source_typed_parquet` (1,044,848 rows).

**Conservation**: Raw (1,067,371) = Typed (1,044,848) + Exclusions (22,523) — verified in Check 1.

---

## 4. Classification Rules and Exception Categories

Each typed source row is classified into exactly one of four categories:

| Category | Rule | Count |
| --- | --- | --- |
| **SALE** | Quantity > 0 and valid invoice, price, customer (nullable), stock code | 1,015,251 |
| **CANCELLATION** | Quantity < 0 (negative quantity indicates return/cancellation) | 18,191 |
| **ADJUSTMENT** | Rows not meeting SALE/CANCELLATION criteria but with valid data (e.g., zero-quantity manual adjustments) | 5,377 |
| **EXCEPTION** | Rows with invalid or missing critical fields (e.g., null invoice, non-numeric quantity, malformed price) | 6,029 |

**Conservation**: Typed (1,044,848) = Ledger (1,033,442) + Adjustments (5,377) + Exceptions (6,029) — verified in Check 2.
**Exclusivity**: Each row_id appears in exactly one classification output — verified in Check 4.

---

## 5. FIFO Matching Semantics and Unmatched Reasons

### Matching Algorithm

The FIFO (First-In-First-Out) allocation matches cancellation transactions to prior sales within the same matching key group.

* **Grouping key**: `customer_id` + `UPPER(TRIM(stock_code))` + `unit_price` (decimal exact match)
* **Null customer handling**: Cancellations with null customer_id are never processed through FIFO; they receive explicit `UNMATCHED` / `MISSING_CUSTOMER_ID` status.
* **Same-timestamp handling**: At the same `invoice_date`, cancellations are processed before sales (priority 0 < 1), ensuring only strictly prior sales are eligible.
* **FIFO queue**: Sales add inventory to a deque (FIFO order). Cancellations consume from the front of the queue.
* **Partial matching**: If a cancellation cannot be fully matched, it receives `PARTIALLY_MATCHED` status. Unmatched remainder is never carried forward.
* **Over-allocation prevention**: Each sale lot's remaining quantity is tracked; no lot can be allocated beyond its original quantity.
* **Allocation IDs**: Deterministic SHA-256 hash of `sale_row_id||cancellation_row_id`.
* **Currency**: All financial values use `Decimal` arithmetic (no floating point).

### Implementation

The algorithm is implemented via `groupBy(customer_id, _match_stock_code, price).applyInPandas(process_fifo_group)`. Each group is serialized to pandas, processed with a FIFO deque, and the results are returned to Spark. A staging Parquet write/read replaces `.persist()` (not supported on Serverless).

### Cancellation Status Breakdown

| Status | Reason | Rows | Requested Qty | Matched Qty | Unmatched Qty |
| --- | --- | --- | --- | --- | --- |
| MATCHED | — | 15,230 | 431,678 | 431,678 | 0 |
| PARTIALLY_MATCHED | INSUFFICIENT_PRIOR_QUANTITY | 110 | 2,964 | 1,625 | 1,339 |
| UNMATCHED | MISSING_CUSTOMER_ID | 494 | 3,860 | 0 | 3,860 |
| UNMATCHED | NO_AVAILABLE_PRIOR_QUANTITY | 64 | 526 | 0 | 526 |
| UNMATCHED | NO_PRIOR_ELIGIBLE_SALE | 2,293 | 31,082 | 0 | 31,082 |

---

## 6. Financial Reconciliation Definitions

### Phase 5 Tests (Cell 16) — All 6 Passed

| Test | Description | Result |
| --- | --- | --- |
| A | Invoice-to-monthly-country reconciliation (all measures match) | PASS |
| B | Net-value identity: net = gross − cancellation in both outputs | PASS |
| C | Source row conservation: line counts match ledger (1,015,251 sales, 18,191 cancellations) | PASS |
| D | Cancellation value and quantity reconcile across all outputs | PASS |
| E | Customer history: unique keys, first/prev dates, intervals, non-decreasing lifetime values, no future leakage | PASS |
| F | Persisted outputs read back with correct schemas and unique keys | PASS |

### Phase 6 End-to-End Checks (Cell 17) — All 14 Passed

| # | Check | Result |
| --- | --- | --- |
| 1 | Raw rows = typed + exclusions (1,067,371 = 1,044,848 + 22,523) | PASS |
| 2 | Typed rows = ledger + adjustments + exceptions (1,044,848 = 1,033,442 + 5,377 + 6,029) | PASS |
| 3 | Sales + cancellations = ledger (1,015,251 + 18,191 = 1,033,442) | PASS |
| 4 | Row ID exclusivity across classification outputs (1,044,848 unique) | PASS |
| 5 | No null or duplicate row IDs in any dataset | PASS |
| 6 | Every cancellation has exactly one status record (18,191 = 18,191) | PASS |
| 7 | requested_quantity = matched + unmatched for all cancellations | PASS |
| 8 | No allocation exceeds sale's original quantity | PASS |
| 9 | Sale timestamps strictly earlier than cancellation timestamps | PASS |
| 10 | Allocation totals reconcile with matched quantities (433,303 = 433,303) | PASS |
| 11 | Missing-customer cancellations remain unmatched (494 all UNMATCHED) | PASS |
| 12 | Invoice & monthly-country totals match ledger | PASS |
| 13 | Customer purchase-history keys unique (36,669) | PASS |
| 14 | Net value = gross − cancellation (18,941,301.39 = 19,713,221.70 − 771,920.31) | PASS |

### Baseline Count Verification

All 12 recorded baseline counts match the current persisted datasets exactly. No discrepancies found.

---

## 7. Idempotency Test Results

**Method**: The FIFO allocation and financial reporting were re-run against unchanged inputs using dedicated test output paths. Original outputs were not modified.

### Deterministic Fingerprints

| Output | Original Fingerprint | Rerun Fingerprint | Match |
| --- | --- | --- | --- |
| Allocations (MD5 of sorted key columns) | 6108fc0909842e87c645860ea3d603c8 | 6108fc0909842e87c645860ea3d603c8 | ✓ |
| Cancellation status (MD5 of sorted key columns) | ee07eab6541983be33801469c14b3860 | ee07eab6541983be33801469c14b3860 | ✓ |
| Invoice summary (MD5 of sorted key columns) | c07b68a4ebf7d0afde43f25b6492506f | c07b68a4ebf7d0afde43f25b6492506f | ✓ |

### Timing

| Stage | Elapsed |
| --- | --- |
| FIFO allocation rerun | 1,052.4s |
| Financial reporting rerun | 0.4s |

### Conclusion

The pipeline is fully deterministic and idempotent. Repeated execution produces identical allocation IDs, quantities, statuses, and financial totals. No records are duplicated or totals inflated.

---

## 8. Benchmark Methodology and Observed Metrics

### Bottleneck Analysis

The FIFO allocation uses `applyInPandas`, which serializes each group to pandas via Arrow, processes it in Python (single-threaded per group), and returns results to Spark. The groupBy shuffle distributes 805,979 matchable rows across 501,527 groups.

**Key finding**: 486,088 of 501,527 groups (96.9%) contain only sales and no cancellations. These groups produce zero allocation or status records but still incur full Arrow serialization and Python processing overhead.

### Optimization: Pre-Filter to Groups with Cancellations

The optimized version identifies the 15,439 groups that contain at least one cancellation and inner-joins the matchable rows to only those group keys before calling `applyInPandas`. This removes 748,939 of 805,979 rows (92.9%) before the expensive Python stage.

### Results

| Metric | Baseline | Optimized | Improvement |
| --- | --- | --- | --- |
| Input rows to applyInPandas | 805,979 | 57,040 | 92.9% reduction |
| Groups processed | 501,527 | 15,439 | 96.9% reduction |
| Allocation count | 16,339 | 16,339 | Identical |
| Status count | 18,191 | 18,191 | Identical |
| Elapsed time | 1,045.7s | 40.1s | **26.07x speedup** |

### Correctness Equivalence

Both implementations produce identical allocation and status outputs, verified by deterministic MD5 fingerprints. Both also match the original persisted outputs. The optimized version is recommended for future pipeline runs.

### Metrics Note

Spark Serverless does not expose per-stage shuffle read/write or spill metrics through the standard `SparkContext` API (unavailable on Spark Connect). The observed wall-clock elapsed time is the primary measurable metric. The `df.explain()` plan was inspected and confirms the groupBy + applyInPandas execution pattern. No spill metrics were available for capture.

---

## 9. Known Limitations and Unresolved Risks

1. **Same-timestamp invoice lines**: Some invoices have lines at different timestamps. The customer purchase history's cumulative lifetime values are computed per timestamp. Late-added lines from an earlier invoice may not be included in that invoice's cumulative total. This is by design (cumulative as-of purchase timestamp).

2. **No production deployment**: The pipeline runs on development Serverless compute only. No production resources, schedules, jobs, or clusters have been created. The Declarative Automation Bundle has not been deployed.

3. **Performance**: The baseline FIFO allocation takes ~17 minutes. The optimized pre-filtered version achieves 26x speedup (~40 seconds) but has not been deployed as the production logic. The optimization is validated for correctness but the working cell 10 remains unchanged.

4. **Spark Connect limitations**: `SparkContext`-level metrics (shuffle bytes, spill memory, executor details) are not available on Serverless Spark Connect. Performance analysis relies on wall-clock timing and query plan inspection.

5. **Data freshness**: All outputs are derived from the original Excel workbook snapshot. No incremental or streaming data ingestion is configured.

6. **Single-notebook architecture**: The entire pipeline is in one notebook. For production, splitting into modular notebooks or a Lakeflow Spark Declarative Pipeline would be recommended.

7. **Serverless compatibility**: `.persist()` / `StorageLevel` is not supported on Serverless. The staging Parquet approach is used instead. This is a known constraint, not a risk.

---

## 10. Reproducible Instructions

### Prerequisites

* Databricks workspace with Serverless compute enabled
* Unity Catalog volume `portfolio_projects.retail_sales.raw` containing source workbook and all intermediate Parquet outputs
* Notebook `01_environment_check` (ID: 2863572223322685)

### Execution Steps

1. Open notebook `01_environment_check` in the Databricks workspace
2. Attach to Serverless compute (GPU or CPU backed)
3. Run cells in order:
   - Cells 1–8: Source extraction and typing (reads XLSX, writes raw/typed Parquet)
   - Cell 9: Transaction classification (reads typed, writes ledger/adjustments/exceptions)
   - Cell 10: FIFO allocation (reads ledger, writes allocations + status via staging)
   - Cell 11: Synthetic FIFO tests (6 scenarios, validates algorithm correctness)
   - Cell 12: Post-write validation (counts, over-allocation, timestamps, uniqueness)
   - Cells 13–14: Financial reporting (invoice summary, monthly-country sales)
   - Cell 15: Customer purchase history
   - Cell 16: Financial reconciliation tests A–F
   - Cell 17: Phase 6 end-to-end row-accounting checks (14 checks)
   - Cell 18: Phase 6 idempotency test (FIFO rerun + fingerprint comparison)
   - Cell 19: Phase 6 performance benchmark (baseline vs optimized)
4. Review outputs in `/Volumes/portfolio_projects/retail_sales/raw/`

### Re-run from Any Stage

The pipeline is idempotent. Any stage can be re-run against its persisted inputs without affecting upstream or downstream outputs. All intermediate results are persisted to Parquet.

---

## 11. Verification Status: Tests Run vs Not Run

### Tests Actually Run and Passed

* 6 synthetic FIFO correctness tests (Cell 11): scenarios A–F
* Post-write FIFO validation (Cell 12): counts, over-allocation, timestamp ordering, allocation ID uniqueness
* 6 financial reconciliation tests (Cell 16): Tests A–F
* 14 end-to-end row-accounting checks (Cell 17): Checks 1–14
* Idempotency test (Cell 18): FIFO rerun, fingerprint comparison, financial reporting rerun
* Performance benchmark (Cell 19): Baseline vs optimized, correctness equivalence validation

### Tests NOT Run

* No load/stress testing has been performed
* No production deployment or integration testing
* No multi-user or concurrent access testing
* No data quality monitoring or alerting configured
* No automated CI/CD pipeline validation
* Spark shuffle/spill metrics were not available for capture on Serverless Spark Connect

---

## 12. Summary

The RetailRecon pipeline is **development-validated**. All 20 quality gates (6 Phase 5 + 14 Phase 6) pass. The pipeline is fully deterministic and idempotent. A 26x performance optimization has been identified, validated for correctness, and documented, but not yet deployed as the working implementation. No source or production resources were modified during Phase 6.

The project is ready for review and potential optimization deployment. It is not claimed to be production-ready.
