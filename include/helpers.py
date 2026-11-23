import logging
from pathlib import Path

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent / "sql"
MAX_ERROR_CHARS = 4000


def _snowflake_hook(conn_id: str):
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=conn_id)


def _load_sql(filename: str, params: dict = None) -> str:
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    sql = path.read_text()
    if params:
        for key, val in params.items():
            sql = sql.replace(f"{{{{{key}}}}}", str(val))
    return sql


def _sql_str(value) -> str:
    """Escape a value for use inside a single-quoted Snowflake string literal."""
    return str(value).replace("\\", "\\\\").replace("'", "''")


def run_sql(filename: str, params: dict = None, conn_id: str = "snowflake_conn") -> None:
    sql = _load_sql(filename, params)
    hook = _snowflake_hook(conn_id)
    # split_statements=True allows multiple SQL statements in one file
    hook.run(sql, split_statements=True)


def run_sql_audited(
    script_name: str,
    sql_file: str,
    params: dict,
    conn_id: str = "snowflake_conn",
) -> None:
    """
    Run a SQL file and record it in C_BATCH_LOG:
    RUNNING at start, then SUCCESS or FAILED at the end.
    A problem while writing the log never hides the real error.
    """
    audit_params = {**params, "script_name": script_name}

    run_sql("control/log_start.sql", audit_params, conn_id)

    try:
        run_sql(sql_file, audit_params, conn_id)
    except Exception as exc:
        try:
            message = _sql_str(str(exc)[:MAX_ERROR_CHARS])
            run_sql("control/log_failed.sql", {**audit_params, "error_message": message}, conn_id)
        except Exception:
            log.exception("Could not write the FAILED row to C_BATCH_LOG")
        raise

    try:
        run_sql("control/log_success.sql", audit_params, conn_id)
    except Exception:
        # The load itself succeeded; don't fail the task over the log row.
        log.exception("Could not write the SUCCESS row to C_BATCH_LOG")


def run_query(filename: str, params: dict = None, conn_id: str = "snowflake_conn") -> tuple:
    sql = _load_sql(filename, params)
    hook = _snowflake_hook(conn_id)
    return hook.get_first(sql)


def run_query_all(filename: str, params: dict = None, conn_id: str = "snowflake_conn") -> list:
    """Run a query file and return every row."""
    sql = _load_sql(filename, params)
    hook = _snowflake_hook(conn_id)
    return hook.get_records(sql)