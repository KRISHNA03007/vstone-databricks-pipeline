"""
Unit tests for src/notebooks/bronze/01_chunk1_initial.ipynb.

Unlike tests/test_02_bronze_transactions1.py (which is an *integration* test
that needs a live Databricks SQL warehouse + env vars), these are true, fast,
offline UNIT tests: they load the notebook's functions in isolation (see
notebook_loader.py) and drive them with `unittest.mock` fakes for `spark`
and `dbutils`. No cluster, no network, no secrets required - they run on
every PR via `pytest tests/`.

What's covered:
  - get_parameters(): widget wiring and default values (no hardcoded values
    leak into the returned dict; everything flows through dbutils widgets).
  - ensure_schema_exists(): emits the correct, parameterized CREATE SCHEMA
    statement for whatever catalog/schema is passed in (not hardcoded).
  - create_bronze_table(): the Bronze DDL contains every core contract
    column, the two audit columns (load_dt, source), DELTA format, and the
    column-mapping table properties - parameterized by target_table/source_path.
  - ingest_data_copy_into(): emits an idempotent COPY INTO with mergeSchema
    enabled, parameterized by target_table/source_path.
"""
import os
import re
from unittest import mock

import pytest

from notebook_loader import load_notebook_definitions

NOTEBOOK_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "src",
    "notebooks",
    "bronze",
    "01_chunk1_initial.ipynb",
)

CORE_CONTRACT_COLUMNS = [
    "Transaction unique identifier",
    "Price",
    "Date of Transfer",
    "Property Type",
    "Old/New",
    "Duration",
    "Town/City",
    "District",
    "County",
    "PPDCategory Type",
    "Record Status - monthly file only",
]


def _make_fake_dbutils(overrides=None):
    """
    A dbutils fake that behaves like the real widget API closely enough for
    get_parameters(): `.text(name, default)` registers a widget (remembering
    its default unless overridden), and `.get(name)` returns the stored value.
    """
    overrides = overrides or {}
    store = {}

    fake = mock.MagicMock()

    def _text(name, default):
        store[name] = overrides.get(name, default)

    def _get(name):
        return store[name]

    fake.widgets.text.side_effect = _text
    fake.widgets.get.side_effect = _get
    return fake


@pytest.fixture
def fake_spark():
    """A MagicMock spark whose .sql() calls we can inspect after the fact."""
    return mock.MagicMock()


@pytest.fixture
def notebook_ns(fake_spark):
    """
    Load the notebook's functions with a fresh fake dbutils/spark for every
    test (so tests can't leak widget state or SQL call history into each other).
    """
    def _load(dbutils_overrides=None):
        return load_notebook_definitions(
            NOTEBOOK_PATH,
            injected_globals={
                "spark": fake_spark,
                "dbutils": _make_fake_dbutils(dbutils_overrides),
            },
        )

    return _load


# ---------------------------------------------------------------------------
# get_parameters()
# ---------------------------------------------------------------------------

def test_get_parameters_returns_documented_defaults(notebook_ns):
    """With no overrides, get_parameters() must return exactly the Day-1 defaults."""
    ns = notebook_ns()
    params = ns["get_parameters"]()

    assert params == {
        "source_path": "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz",
        "catalog": "vstone",
        "schema": "bronze",
        "table_name": "transactions",
    }


def test_get_parameters_honors_widget_overrides(notebook_ns):
    """
    get_parameters() must read back whatever value is actually registered on
    the widget - proving the function is parameterized (job/task base_parameters
    can override it) rather than hardcoding the defaults.
    """
    ns = notebook_ns(dbutils_overrides={
        "source_path": "/Volumes/vstone/landing/raw_data/chunks/chunk2_incremental.csv.gz",
        "catalog": "vstone",
        "schema": "bronze",
        "table_name": "transactions1",
    })
    params = ns["get_parameters"]()

    assert params["source_path"] == "/Volumes/vstone/landing/raw_data/chunks/chunk2_incremental.csv.gz"
    assert params["table_name"] == "transactions1"


def test_get_parameters_registers_all_four_widgets(notebook_ns, fake_spark):
    """Sanity check that all 4 expected widgets are actually registered (not 3, not 5)."""
    ns = notebook_ns()
    dbutils = ns["dbutils"]
    ns["get_parameters"]()

    registered = {call.args[0] for call in dbutils.widgets.text.call_args_list}
    assert registered == {"source_path", "catalog", "schema", "table_name"}


# ---------------------------------------------------------------------------
# ensure_schema_exists()
# ---------------------------------------------------------------------------

def test_ensure_schema_exists_is_parameterized_not_hardcoded(notebook_ns, fake_spark, capsys):
    """
    The CREATE SCHEMA statement must use whatever catalog/schema are passed
    in - calling it with different arguments must change the SQL emitted,
    proving there is no hardcoded 'vstone.bronze' baked into the function.
    """
    ns = notebook_ns()
    ns["ensure_schema_exists"]("some_other_catalog", "some_other_schema")

    fake_spark.sql.assert_called_once()
    sql_text = fake_spark.sql.call_args.args[0]
    assert "CREATE SCHEMA IF NOT EXISTS some_other_catalog.some_other_schema" in sql_text

    out = capsys.readouterr().out
    assert "some_other_catalog.some_other_schema" in out


def test_ensure_schema_exists_default_target(notebook_ns, fake_spark):
    """Day-1 default target: vstone.bronze schema must be created idempotently (IF NOT EXISTS)."""
    ns = notebook_ns()
    ns["ensure_schema_exists"]("vstone", "bronze")

    sql_text = fake_spark.sql.call_args.args[0]
    assert "CREATE SCHEMA IF NOT EXISTS vstone.bronze" in sql_text
    assert "IF NOT EXISTS" in sql_text  # idempotent / safe to re-run


# ---------------------------------------------------------------------------
# create_bronze_table()
# ---------------------------------------------------------------------------

def test_create_bronze_table_ddl_is_parameterized(notebook_ns, fake_spark):
    """The DDL must reference the exact target_table/source_path passed in, not a fixed one."""
    ns = notebook_ns()
    target_table = "vstone.bronze.transactions_test"
    source_path = "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz"

    ns["create_bronze_table"](target_table, source_path)

    ddl = fake_spark.sql.call_args.args[0]
    assert f"CREATE TABLE IF NOT EXISTS {target_table}" in ddl
    assert source_path in ddl  # used as the `source` column's DEFAULT


def test_create_bronze_table_contains_all_core_contract_columns(notebook_ns, fake_spark):
    """Every core business column from the data contract must appear in the DDL."""
    ns = notebook_ns()
    ns["create_bronze_table"]("vstone.bronze.transactions", "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz")

    ddl = fake_spark.sql.call_args.args[0]
    missing = [c for c in CORE_CONTRACT_COLUMNS if f"`{c}`" not in ddl]
    assert not missing, f"DDL is missing core contract columns: {missing}"


def test_create_bronze_table_has_audit_columns_with_defaults(notebook_ns, fake_spark):
    """load_dt/source audit columns must exist with sensible DEFAULTs (mandatory per spec)."""
    ns = notebook_ns()
    ns["create_bronze_table"]("vstone.bronze.transactions", "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz")

    ddl = fake_spark.sql.call_args.args[0]
    assert "`load_dt` TIMESTAMP DEFAULT current_timestamp()" in ddl
    assert "`source` STRING DEFAULT" in ddl


def test_create_bronze_table_is_delta_with_column_mapping(notebook_ns, fake_spark):
    """Table must be DELTA and use column-mapping mode 'name' (required for schema evolution + spaces in col names)."""
    ns = notebook_ns()
    ns["create_bronze_table"]("vstone.bronze.transactions", "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz")

    ddl = fake_spark.sql.call_args.args[0]
    assert "USING DELTA" in ddl
    assert "'delta.columnMapping.mode' = 'name'" in ddl


def test_create_bronze_table_is_idempotent(notebook_ns, fake_spark):
    """Must use CREATE TABLE IF NOT EXISTS so reruns (multiple run scenarios) don't fail."""
    ns = notebook_ns()
    ns["create_bronze_table"]("vstone.bronze.transactions", "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz")

    ddl = fake_spark.sql.call_args.args[0]
    assert re.search(r"CREATE TABLE IF NOT EXISTS", ddl)


# ---------------------------------------------------------------------------
# ingest_data_copy_into()
# ---------------------------------------------------------------------------

def test_ingest_data_copy_into_is_parameterized(notebook_ns, fake_spark):
    """COPY INTO must target/source exactly what's passed in - no hardcoded path/table."""
    ns = notebook_ns()
    target_table = "vstone.bronze.transactions_test"
    source_path = "/Volumes/vstone/landing/raw_data/chunks/chunk2_incremental.csv.gz"

    ns["ingest_data_copy_into"](target_table, source_path)

    sql_text = fake_spark.sql.call_args.args[0]
    assert f"COPY INTO {target_table}" in sql_text
    assert f"FROM '{source_path}'" in sql_text


def test_ingest_data_copy_into_uses_csv_with_header_and_merge_schema(notebook_ns, fake_spark):
    """Must read CSV with header=true and allow mergeSchema so incremental/evolved chunks don't break ingestion."""
    ns = notebook_ns()
    ns["ingest_data_copy_into"]("vstone.bronze.transactions", "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz")

    sql_text = fake_spark.sql.call_args.args[0]
    assert "FILEFORMAT = CSV" in sql_text
    assert "'header' = 'true'" in sql_text
    assert "'mergeSchema' = 'true'" in sql_text


def test_ingest_data_copy_into_is_idempotent_by_design(notebook_ns, fake_spark):
    """
    COPY INTO is natively idempotent (Databricks tracks already-loaded files),
    so calling it twice for the same file must not raise and must issue the
    same SQL both times - proving the notebook handles multiple run scenarios
    (a Day-1 requirement) without extra guard code needed.
    """
    ns = notebook_ns()
    target_table = "vstone.bronze.transactions"
    source_path = "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz"

    ns["ingest_data_copy_into"](target_table, source_path)
    ns["ingest_data_copy_into"](target_table, source_path)

    assert fake_spark.sql.call_count == 2
    first_call, second_call = fake_spark.sql.call_args_list
    assert first_call == second_call


# ---------------------------------------------------------------------------
# End-to-end wiring (functions called together, as __main__ does)
# ---------------------------------------------------------------------------

def test_pipeline_functions_compose_without_error(notebook_ns, fake_spark):
    """
    Exercises the same call sequence as the notebook's `if __name__ == "__main__"`
    block (get_parameters -> ensure_schema_exists -> create_bronze_table ->
    ingest_data_copy_into) end-to-end against mocks, to catch any signature
    mismatch between the functions before it ever reaches a real job run.
    """
    ns = notebook_ns()
    params = ns["get_parameters"]()
    target_table = f"{params['catalog']}.{params['schema']}.{params['table_name']}"

    ns["ensure_schema_exists"](params["catalog"], params["schema"])
    ns["create_bronze_table"](target_table, params["source_path"])
    ns["ingest_data_copy_into"](target_table, params["source_path"])

    assert fake_spark.sql.call_count == 3  # CREATE SCHEMA, CREATE TABLE, COPY INTO