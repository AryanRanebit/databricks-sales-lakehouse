# Learning notes

## What this project is
A small pipeline that takes messy order files and turns them into a clean star schema you can query.
Data is fake, made by `data/generate_data.py` (no outside dataset, so no licence issue).

## The three layers (medallion)
| Layer | Job | File |
|---|---|---|
| Bronze | Load raw files untouched, all text | `sql/01_bronze.sql` |
| Silver | Clean, type, dedupe, reject bad rows | `sql/02_silver.sql` |
| Gold | Star schema for analysis | `sql/03_gold.sql` |

## Kimball star schema in one minute
`fact_sales` holds numbers (quantity, amount) at one grain: one order line.
`dim_customer`, `dim_product`, `dim_date` describe them. Facts point to dimensions by surrogate key.

## Problems planted in the data, and where each is handled
- Duplicate customer row -> `DISTINCT` in silver
- Messy city text ("mumbai", " THANE") -> TRIM + casing in silver
- Missing unit price -> filled from list price (a written business rule)
- Orders for a customer that does not exist -> moved to `silver_rejected_orders`

## Incremental loading (batch2)
Real pipelines get new files over time. `data/raw/batch1` is the first load; `batch2` is a later delivery.
- Bronze appends each batch with a `batch_id`. A batch already in `bronze_batches` is skipped, so re-running does nothing.
- Silver uses `MERGE`: update the row if the key exists, insert if not (same statement exists in Delta Lake).
- Each batch runs in one transaction. If it fails halfway, nothing is left behind (tested).
- Fact rows are inserted only for orders not yet in the fact. Status changes update status only.

## SCD Type 2 on dim_customer
When a customer moves city we do not overwrite the row. We close the old version (`valid_to`) and add a new one (`valid_from`).
Each fact points to the version valid ON THE ORDER DATE, so last year's sales still show the old city and old facts are never re-pointed.
Simplification: the change date is the batch's `as_of` date, not a date from the source system.
`dim_product` is Type 1 on purpose (price change = overwrite) so you can compare the two.
Known limits: orders for a customer who arrives in a LATER batch stay rejected (not retried); a customer's earlier orders are not re-mapped if a change is back-dated.

## Tests
`sql/04_checks.sql` are data-quality checks. `tests/test_pipeline.py` proves the rules work.
Run: `pip install -r requirements.txt && python data/generate_data.py && pytest -q && python pipeline.py`
Tests prove behaviour: rerun is a no-op, 8 movers get 2 versions, re-sent unchanged customers get none, old facts keep the old city, a broken batch rolls back.

## Status (be honest in interviews)
- Local DuckDB version: tested (pytest + SQL checks).
- Databricks Free Edition: the Spark SQL notebooks in `databricks/` were run in a real workspace on serverless compute.
  batch1 and batch2 loaded, a second batch2 run was skipped, and the 13 quality checks passed. Counts match the local run.
- Not done: a scheduled Databricks Job, deploy through bundles or CI (Free Edition has no service principals), failure testing on Databricks.
- Differences you should be able to explain: Delta has no multi-statement transactions (steps are retry-safe instead);
  MERGE replaces UPDATE ... FROM; date functions are `date_format` and `sequence` instead of `strftime` and `generate_series`.
- Not used: Azure Data Factory, Synapse, ADLS, T-SQL. Databricks notebooks play the orchestration role.
- The GitHub Actions workflow only runs the local tests; it has not been run yet.
