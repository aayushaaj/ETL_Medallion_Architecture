import pendulum

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, Variable, task, get_current_context, TaskGroup

from include.helpers import run_query, run_query_all, run_sql, run_sql_audited

SNOWFLAKE_CONN_ID = "snowflake_conn"
DEFAULT_DATA_PATH = "/opt/airflow/data"
# Retention policy: number of days of batch data to retain.
# Set to -1 or a very high number to effectively disable purging
DEFAULT_RETENTION_DAYS = 365
ENABLE_PURGE_DEFAULT = False

SOURCE_FILES = [
    "pos_sales",
    "inventory_snapshot",
    "item_master",
    "store_master",
]

# Silver layer tables
TMP_MASTER_TABLES = [
    "item_master",
    "store_master",
]

TMP_TRANSACTION_TABLES = [
    "pos_sales",
    "inventory_snapshot",
]

# Gold layer tables
#Reference dimensions - no dependency on another Gold table.
GOLD_REF_TABLES = [
    "subclass",
    "district",
    "day",
    "customer",
]

#Core dimensions - look up the reference dimensions' keys.
GOLD_DIM_TABLES = [
    "item_master",
    "store_master",
]

#Facts - look up every dimension key.
GOLD_FACT_TABLES = [
    "pos_sales",
    "inventory_snapshot",
    "price",
]

# 4. Marts - aggregate the facts.
GOLD_MART_TABLES = [
    "sales_daily",
]


def get_batch_metadata() -> tuple[str, str]:
    """
    Retrieve batch date and DAG run ID from the current
    Airflow execution context.

    Returns:
        batch_date: Logical execution date (YYYY-MM-DD)
        dag_run_id: Current DAG run identifier
    """
    context = get_current_context()
    batch_date = context["ds"]
    dag_run_id = context["run_id"]
    return batch_date, dag_run_id


def get_retention_cutoff() -> str:
    """
    Calculate the batch_date cutoff for data retention.

    Returns:
        cutoff_date: Rows with _batch_date before this date will be purged.
    """
    try:
        retention_days = int(Variable.get("retention_days"))
    except Exception:
        retention_days = DEFAULT_RETENTION_DAYS
    if retention_days < 0 or retention_days > 1825:  # > 5 years
        print(f"Retention days ({retention_days}) disabling batch purging.")
        return None
    cutoff = pendulum.now().subtract(days=retention_days)
    return cutoff.format("YYYY-MM-DD")


# DAG DEFINITION
with DAG(
    dag_id="retail_medallion_load",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "silver", "dw"],
) as dag:

    # BRONZE LAYER
    @task
    def load_stg(file_name: str) -> None:
        """
        Load one source file into its Bronze STG table.
        One mapped instance is created per source file.
        """
        batch_date, dag_run_id = get_batch_metadata()

        data_path = Variable.get(
            "data_path",
            default=DEFAULT_DATA_PATH,
        )

        run_sql_audited(
            script_name=f"load_stg_{file_name}",
            sql_file=f"bronze/load_stg_{file_name}.sql",
            params={
                "batch_date": batch_date,
                "data_path": data_path,
                "run_id": dag_run_id,
            },
            conn_id=SNOWFLAKE_CONN_ID,
        )

    @task
    def schema_gate() -> None:
        """
        Quality Gate QG1:
        Check whether Bronze loading successfully
        loaded rows for the current batch.
        """
        batch_date, _ = get_batch_metadata()

        row = run_query(
            "bronze/schema_gate.sql",
            {"batch_date": batch_date},
            conn_id=SNOWFLAKE_CONN_ID,
        )

        if not row or row[0] == 0:
            raise AirflowFailException(
                f"QG1 failed for {batch_date}: no rows loaded."
            )

        print(f"QG1 passed for batch {batch_date}.")

    # SILVER LAYER
    @task
    def load_tmp(table_name: str) -> str:
        """
        Load one table into the Silver TMP layer.
        Used for both master and transaction tables.
        """
        batch_date, dag_run_id = get_batch_metadata()

        run_sql_audited(
            script_name=f"load_tmp_{table_name}",
            sql_file=f"silver/load_tmp_{table_name}.sql",
            params={
                "batch_date": batch_date,
                "run_id": dag_run_id,
            },
            conn_id=SNOWFLAKE_CONN_ID,
        )

        return batch_date

    @task
    def validate_quarantine() -> None:
        """
        Validate and report rejected/quarantined rows
        for the current batch.
        """
        batch_date, _ = get_batch_metadata()

        row = run_query(
            "silver/validate_quarantine.sql",
            {"batch_date": batch_date},
            conn_id=SNOWFLAKE_CONN_ID,
        )

        print(f"Batch {batch_date}: Rejected {row[0]} rows.")

    # GOLD LAYER
    @task
    def load_tgt(table_name: str) -> None:
        """
        Load one Gold table from its Silver TMP source.
        Each dependency level is expanded separately so that
        dimensions exist before the facts look up their keys.
        """
        batch_date, dag_run_id = get_batch_metadata()

        run_sql_audited(
            script_name=f"load_tgt_{table_name}",
            sql_file=f"gold/load_tgt_{table_name}.sql",
            params={
                "batch_date": batch_date,
                "run_id": dag_run_id,
            },
            conn_id=SNOWFLAKE_CONN_ID,
        )

    # RECONCILIATION LAYER
    @task
    def reconcile() -> None:
        """
        Compare Silver and Gold row counts for the batch
        and fail the run on any mismatch.
        """
        batch_date, _ = get_batch_metadata()

        rows = run_query_all(
            "reconcile/validate_reconciliation.sql",
            {"batch_date": batch_date},
            conn_id=SNOWFLAKE_CONN_ID,
        ) or []

        failures = []
        for check_name, source_rows, target_rows, result in rows:
            print(f"{result:<4} {check_name:<14} silver={source_rows} gold={target_rows}")
            if result != "PASS":
                failures.append(check_name)

        if failures:
            raise AirflowFailException(
                f"Reconciliation failed for {batch_date}: {', '.join(failures)}"
            )

        print(f"Reconciliation passed for batch {batch_date}.")

    @task
    def record_batch_completion() -> None:
        """
        Record batch completion in C_LOAD_DATES.
        Idempotent: safe to re-run for the same batch_date.
        """
        batch_date, dag_run_id = get_batch_metadata()

        run_sql(
            "control/update_c_load_dates.sql",
            {"batch_date": batch_date, "run_id": dag_run_id},
            conn_id=SNOWFLAKE_CONN_ID,
        )

        print(f"Recorded completion in C_LOAD_DATES for batch {batch_date}.")

@task
    def purge_old_batches() -> None:
        \n        # Check if purge is enabled via Airflow Variable
        try:
            enable_purge = Variable.get("enable_purge", default_var=str(ENABLE_PURGE_DEFAULT))
            if enable_purge.lower() not in ["true", "1", "yes"]:
                print(f"Purge disabled via enable_purge Variable. Set to true/1/yes to enable.")
                return
        except Exception:
            if not ENABLE_PURGE_DEFAULT:
                print(f"Purge disabled by default (ENABLE_PURGE_DEFAULT=False).")
                return
        try:
            retention_days = int(Variable.get("retention_days"))
        except Exception:
            retention_days = DEFAULT_RETENTION_DAYS
        # Disable purging if retention_days is set to -1 or very high
        if retention_days < 0 or retention_days > 1825:  # > 5 years
            print(f"Retention days ({retention_days}) disabling batch purging.")
            return
        cutoff = get_retention_cutoff()
        print(f"Purging data older than cutoff date: {cutoff}")\n        # Bronze layer: STG_* tables
        run_sql("purge_bronze.sql", {"cutoff": cutoff}, conn_id=SNOWFLAKE_CONN_ID)
        print(f"  Purged Bronze STG tables up to cutoff {cutoff}.")\n        # Silver layer: TMP_* + C_REJ_TABLE
        run_sql("purge_silver.sql", {"cutoff": cutoff}, conn_id=SNOWFLAKE_CONN_ID)
        print(f"  Purged Silver TMP tables and C_REJ_TABLE up to cutoff {cutoff}.")\n        # Gold layer: DIM_*, FACT_*, RPT_* tables
        run_sql("purge_gold.sql", {"cutoff": cutoff}, conn_id=SNOWFLAKE_CONN_ID)
        print(f"  Purged Gold tables up to cutoff {cutoff}.")\n
        # Disable purging if retention_days is set to -1 or very high
        if retention_days < 0 or retention_days > 1825:  # > 5 years
            print(f"Retention days ({retention_days}) disabling batch purging.")
            return

        cutoff = get_retention_cutoff()
        print(f"Purging data older than cutoff date: {cutoff}")

        # Bronze layer: STG_* tables
        run_sql("purge_bronze.sql", {"cutoff": cutoff}, conn_id=SNOWFLAKE_CONN_ID)
        print(f"  Purged Bronze STG tables up to cutoff {cutoff}.")

        # Silver layer: TMP_* + C_REJ_TABLE
        run_sql("purge_silver.sql", {"cutoff": cutoff}, conn_id=SNOWFLAKE_CONN_ID)
        print(f"  Purged Silver TMP tables and C_REJ_TABLE up to cutoff {cutoff}.")

        # Gold layer: DIM_*, FACT_*, RPT_* tables
        run_sql("purge_gold.sql", {"cutoff": cutoff}, conn_id=SNOWFLAKE_CONN_ID)
        print(f"  Purged Gold tables up to cutoff {cutoff}.")

    # TASK INSTANTIATION

    wait = FileSensor(
        task_id="wait_for_signal",
        fs_conn_id="file_conn",
        filepath="batch_{{ ds }}/.COMPLETE",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    with TaskGroup("bronze") as bronze:
        stg_tasks = load_stg.expand(file_name=SOURCE_FILES)
        gate = schema_gate()
        stg_tasks >> gate

    with TaskGroup("silver") as silver:
        tmp_master_tasks = load_tmp.override(task_id="load_tmp_master").expand(
            table_name=TMP_MASTER_TABLES,
        )
        tmp_transaction_tasks = load_tmp.override(task_id="load_tmp_transaction").expand(
            table_name=TMP_TRANSACTION_TABLES,
        )
        quar = validate_quarantine()
        tmp_master_tasks >> tmp_transaction_tasks >> quar

    with TaskGroup("gold") as gold:
        tgt_ref_tasks = load_tgt.override(task_id="load_tgt_ref_dims").expand(
            table_name=GOLD_REF_TABLES,
        )
        tgt_dim_tasks = load_tgt.override(task_id="load_tgt_core_dims").expand(
            table_name=GOLD_DIM_TABLES,
        )
        tgt_fact_tasks = load_tgt.override(task_id="load_tgt_facts").expand(
            table_name=GOLD_FACT_TABLES,
        )
        tgt_mart_tasks = load_tgt.override(task_id="load_tgt_marts").expand(
            table_name=GOLD_MART_TABLES,
        )
        tgt_ref_tasks >> tgt_dim_tasks >> tgt_fact_tasks >> tgt_mart_tasks

    recon = reconcile()
    record_completion = record_batch_completion()
    purge = purge_old_batches()

    # TASK DEPENDENCIES
    wait >> bronze >> silver >> gold >> recon >> record_completion >> purge