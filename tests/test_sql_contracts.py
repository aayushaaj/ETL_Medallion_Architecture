import re

from include.helpers import SQL_DIR, _load_sql


def test_all_sql_templates_use_supported_parameters():
    supported_parameters = {
        "batch_date",
        "data_path",
        "error_message",
        "run_id",
        "script_name",
        "cutoff",
    }

    for path in SQL_DIR.rglob("*.sql"):
        relative_path = path.relative_to(SQL_DIR).as_posix()
        template = path.read_text()
        parameters = set(re.findall(r"\{\{(\w+)\}\}", template))

        assert parameters <= supported_parameters, relative_path
        rendered = _load_sql(
            relative_path,
            {parameter: "test-value" for parameter in supported_parameters},
        )
        assert not re.search(r"\{\{\w+\}\}", rendered), relative_path


def test_schema_gate_checks_each_bronze_source_for_the_batch():
    sql = (SQL_DIR / "bronze/schema_gate.sql").read_text()
    expected_tables = {
        "STG_POS_SALES",
        "STG_INVENTORY_SNAPSHOT",
        "STG_ITEM_MASTER",
        "STG_STORE_MASTER",
    }
    checked_tables = set(re.findall(r"RETAIL_SCHEMA\.(STG_\w+)", sql))

    assert checked_tables == expected_tables
    assert sql.count("WHERE _batch_date = '{{batch_date}}'") == len(expected_tables)
    assert "MIN(row_count)" in sql


def test_silver_sql_contains_the_documented_rejection_rules():
    expected_rules = {
        "load_tmp_item_master.sql": [
            "NULL_PK",
            "DUP_PK",
            "INVALID_UNIT_PRICE",
            "INVALID_EFFECTIVE_FROM",
        ],
        "load_tmp_store_master.sql": [
            "NULL_PK",
            "DUP_PK",
            "INVALID_EFFECTIVE_FROM",
        ],
        "load_tmp_pos_sales.sql": [
            "NULL_FK",
            "NULL_FK",
            "NEG_QTY",
            "DUP_LINE",
            "ORPHAN_FK",
        ],
        "load_tmp_inventory_snapshot.sql": [
            "NULL_FK",
            "NULL_FK",
            "NEG_QTY",
            "DUP_LINE",
        ],
    }

    for filename, rules in expected_rules.items():
        sql = (SQL_DIR / "silver" / filename).read_text()
        actual_rules = re.findall(r"THEN '([A-Z_]+)'", sql)
        assert actual_rules == rules * 2, filename


def test_silver_rejects_by_first_matching_rule_only():
    """A row violating multiple rules gets the first matching rule (CASE order).
    Each CASE block appears twice (rejection insert + valid insert), so rules
    repeat; we check the priority order within one block."""
    item_master = (SQL_DIR / "silver" / "load_tmp_item_master.sql").read_text()
    rules = re.findall(r"WHEN .+? THEN '([A-Z_]+)'", item_master)
    assert rules[:4] == [
        "NULL_PK",
        "DUP_PK",
        "INVALID_UNIT_PRICE",
        "INVALID_EFFECTIVE_FROM",
    ]

    pos_sales = (SQL_DIR / "silver" / "load_tmp_pos_sales.sql").read_text()
    rules = re.findall(r"WHEN .+? THEN '([A-Z_]+)'", pos_sales)
    assert rules[:5] == [
        "NULL_FK",
        "NULL_FK",
        "NEG_QTY",
        "DUP_LINE",
        "ORPHAN_FK",
    ]


def test_reconciliation_checks_all_datasets():
    sql = (SQL_DIR / "reconcile" / "validate_reconciliation.sql").read_text()
    expected_checks = {
        "ITEM_MASTER", "STORE_MASTER", "SUBCLASS", "DISTRICT", "CUSTOMER",
        "POS_SALES", "INVENTORY", "PRICE", "SALES_DAILY", "NET_SALES",
    }
    checks = set(re.findall(r"SELECT\s+'([A-Z_]+)'", sql))
    assert checks == expected_checks
    assert "IFF(EQUAL_NULL(" in sql
    assert "'PASS'" in sql
    assert "'FAIL'" in sql


def test_bronze_load_stg_is_idempotent():
    """Each bronze load overwrites the batch before re-loading."""
    for source in ("item_master", "store_master", "pos_sales", "inventory_snapshot"):
        sql = (SQL_DIR / "bronze" / f"load_stg_{source}.sql").read_text()
        assert "OVERWRITE" in sql, source
        assert "FORCE" in sql, source
        assert f"WHERE _batch_date = '{{{{batch_date}}}}'" in sql, source


def test_silver_is_idempotent():
    """Each silver load deletes the batch and its rejections before re-inserting."""
    for source in ("item_master", "store_master", "pos_sales", "inventory_snapshot"):
        sql = (SQL_DIR / "silver" / f"load_tmp_{source}.sql").read_text()
        assert f"DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_{source.upper()}" in sql, source
        assert "DELETE FROM RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE" in sql, source
        assert f"WHERE _batch_date = '{{{{batch_date}}}}'" in sql, source
