import logging

import pytest

from include import helpers


def test_load_sql_substitutes_parameters():
    sql = helpers._load_sql(
        "bronze/schema_gate.sql",
        {"batch_date": "2026-11-20"},
    )

    assert "'2026-11-20'" in sql
    assert "{{batch_date}}" not in sql


def test_load_sql_reports_missing_file():
    with pytest.raises(FileNotFoundError, match="SQL file not found"):
        helpers._load_sql("missing/query.sql")


def test_sql_str_escapes_quotes_and_backslashes():
    assert helpers._sql_str("O'Brien\\data") == "O''Brien\\\\data"


def test_run_sql_audited_records_success(monkeypatch):
    calls = []
    monkeypatch.setattr(
        helpers,
        "run_sql",
        lambda filename, params, conn_id: calls.append((filename, params, conn_id)),
    )

    helpers.run_sql_audited(
        "load_tmp_item_master",
        "silver/load_tmp_item_master.sql",
        {"batch_date": "2026-11-20", "run_id": "run-1"},
        "test_connection",
    )

    assert [call[0] for call in calls] == [
        "control/log_start.sql",
        "silver/load_tmp_item_master.sql",
        "control/log_success.sql",
    ]
    assert all(call[1]["script_name"] == "load_tmp_item_master" for call in calls)
    assert all(call[2] == "test_connection" for call in calls)


def test_run_sql_audited_preserves_load_error_if_failure_log_fails(monkeypatch):
    original_error = RuntimeError("load failed")
    calls = []

    def run_sql(filename, params, conn_id):
        calls.append(filename)
        if filename == "silver/load_tmp_item_master.sql":
            raise original_error
        if filename == "control/log_failed.sql":
            raise OSError("audit unavailable")

    monkeypatch.setattr(helpers, "run_sql", run_sql)

    with pytest.raises(RuntimeError) as error:
        helpers.run_sql_audited(
            "load_tmp_item_master",
            "silver/load_tmp_item_master.sql",
            {"batch_date": "2026-11-20", "run_id": "run-1"},
        )

    assert error.value is original_error
    assert calls == [
        "control/log_start.sql",
        "silver/load_tmp_item_master.sql",
        "control/log_failed.sql",
    ]


def test_run_sql_audited_does_not_fail_after_successful_load(monkeypatch, caplog):
    calls = []

    def run_sql(filename, params, conn_id):
        calls.append(filename)
        if filename == "control/log_success.sql":
            raise OSError("audit unavailable")

    monkeypatch.setattr(helpers, "run_sql", run_sql)

    with caplog.at_level(logging.ERROR):
        helpers.run_sql_audited(
            "load_tmp_item_master",
            "silver/load_tmp_item_master.sql",
            {"batch_date": "2026-11-20", "run_id": "run-1"},
        )

    assert calls == [
        "control/log_start.sql",
        "silver/load_tmp_item_master.sql",
        "control/log_success.sql",
    ]
    assert "Could not write the SUCCESS row" in caplog.text
