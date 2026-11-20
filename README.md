# Retail Medallion ETL Pipeline

A daily batch pipeline that loads retail files into Snowflake (`RETAIL_DW.RETAIL_SCHEMA`) using the medallion architecture. It is orchestrated by the Airflow DAG `retail_medallion_load`, with all logic held in parameterised SQL files.

## What the pipeline implements

```
wait_for_signal -> Bronze (STG) -> QG1 -> Silver (TMP) -> Gold (TGT) -> reconcile -> record_batch_completion
```

| Step                                  | Description                                                                                                                                |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Wait for signal**                   | A sensor waits for `batch_<date>/.COMPLETE` so a batch is never read while it is still being written.                                      |
| **Bronze (`STG_*`)**                  | Loads the four source files as-is (`item_master`, `store_master`, `pos_sales`, `inventory_snapshot`) and tags each row with `_batch_date`. |
| **Quality Gate 1**                    | Checks that Bronze loaded data (see below).                                                                                                |
| **Silver (`TMP_*`)**                  | Validates and de-duplicates. Bad rows go to a quarantine table. Masters load first, then transactions.                                     |
| **Gold (`DIM_*`, `FACT_*`, `RPT_*`)** | Star schema in dependency order: reference dimensions, then item and store dimensions (SCD Type 2), then facts, then the daily sales mart. |
| **Reconcile**                         | Compares Silver and Gold counts and fails the run on any mismatch.                                                                         |
| **Record completion**                 | Writes the batch to `C_LOAD_DATES`.                                                                                                        |

## Quality Gate 1 (QG1)

Runs after Bronze, as the `schema_gate` task. It counts the rows loaded for the batch in each of the four `STG_*` tables and takes the smallest count. If that is `0`, meaning any file loaded nothing, the task raises `AirflowFailException` and the run stops before Silver.

## Quality rules (Silver)

Each Silver table is checked per batch. The first matching rule gives the rejection reason. Rejected rows are written to `C_REJ_TABLE` with the full row as JSON. Valid rows go to the `TMP_*` table. When duplicates exist, the most recently loaded row is kept.

| Table                    | Rule                     | What it catches                                  |
| ------------------------ | ------------------------ | ------------------------------------------------ |
| `TMP_ITEM_MASTER`        | `NULL_PK`                | `item_id` null or blank                          |
|                          | `DUP_PK`                 | repeated `item_id`                               |
|                          | `INVALID_UNIT_PRICE`     | `unit_price < 0`                                 |
|                          | `INVALID_EFFECTIVE_FROM` | `effective_from` null                            |
| `TMP_STORE_MASTER`       | `NULL_PK`                | `location_id` null or blank                      |
|                          | `DUP_PK`                 | repeated `location_id`                           |
|                          | `INVALID_EFFECTIVE_FROM` | `effective_from` null                            |
| `TMP_POS_SALES`          | `NULL_FK`                | `item_id` or `location_id` null or blank         |
|                          | `NEG_QTY`                | `sales_qty < 0`                                  |
|                          | `DUP_LINE`               | repeated `(transaction_id, line_id)`             |
|                          | `ORPHAN_FK`              | `item_id` not in the batch's `TMP_ITEM_MASTER`   |
| `TMP_INVENTORY_SNAPSHOT` | `NULL_FK`                | `item_id` or `location_id` null or blank         |
|                          | `NEG_QTY`                | `on_hand_qty < 0`                                |
|                          | `DUP_LINE`               | repeated `(item_id, location_id, snapshot_date)` |

The `validate_quarantine` task then reports the number of rejected rows for the batch. It does not fail the run.

## Idempotency

Re-running a batch gives the same result and creates no duplicates. Every table carries `_batch_date`, and every script only touches that batch.

| Layer                     | How a re-run is handled                                                                                                                                                                  |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bronze                    | Deletes the batch's rows from the `STG_*` table, then reloads the file (`PUT` with `OVERWRITE`, `COPY INTO` with `FORCE`).                                                               |
| Silver                    | Deletes the batch's rows from the `TMP_*` table and from `C_REJ_TABLE`, then re-inserts.                                                                                                 |
| Gold dimensions and facts | `MERGE` on the business or surrogate key, so existing rows are updated and new rows inserted.                                                                                            |
| SCD2 (items, stores)      | The current row is closed only when a tracked attribute changed. The new version is merged on `item_id` / `location_id` plus `effective_from`, so repeating it creates no extra version. |
| Sales mart                | Rows for the batch's dates are deleted and rebuilt.                                                                                                                                      |
| `C_LOAD_DATES`            | `MERGE` per table. A re-run refreshes the row and never moves the date backwards.                                                                                                        |

The DAG also uses `max_active_runs=1`, so two runs never overlap.

## Recording and audit

Every load task runs through `run_sql_audited`:

1. A `RUNNING` row is written to `C_BATCH_LOG`.
2. The SQL runs.
3. The row is updated to `SUCCESS` (with rows read, inserted and rejected) or `FAILED` (with the error message).

A problem writing the log never hides the real error, and never fails a load that succeeded. When the whole batch finishes, `record_batch_completion` records the batch date and run id for each Gold table in `C_LOAD_DATES`.

## Control tables

| Table             | Purpose                                                                                                                                                                                          |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `C_BATCH_LOG`     | One row per script run: `run_id`, `_batch_date`, `script_name`, `status` (`RUNNING` / `SUCCESS` / `FAILED`), start and end time, `rows_read`, `rows_inserted`, `rows_rejected`, `error_message`. |
| `C_REJ_TABLE`     | Quarantined rows: `_batch_date`, `source_table`, `rule_rejected`, the original row as `payload` (`VARIANT`), `_rejected_at`.                                                                     |
| `C_LOAD_DATES`    | Last completed load per target table: `target_table`, `last_load_date`, `last_run_id`, `updated_at`.                                                                                             |
| `C_PARAM`         | Run parameters (`run_id`, `param_name`, `param_value`) written at the start of each script.                                                                                                      |
| `C_BATCH_SCRIPTS` | Reference list of scripts with their layer (`STG` / `TMP` / `TGT`) and execution order.                                                                                                          |

The tables are created by `include/sql/control/init_control.sql`.
