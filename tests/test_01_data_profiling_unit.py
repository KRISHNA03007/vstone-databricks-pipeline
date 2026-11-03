"""
Unit tests for src/notebooks/data_profiling/01_data_profiling.ipynb.

These are true UNIT tests for the `profile_dataset()` function: they build
small, synthetic PySpark DataFrames in-memory (no Volume, no cluster, no
network) and verify the null-count / null-percentage logic is correct, using
the PySpark-native test utilities `assertDataFrameEqual` and
`assertSchemaEqual` explicitly called for in the project spec.

This complements (does not replace) tests/test_01_dataprofiling.py, which is
an *integration* test that checks the real files landed in the Volume via a
live SQL warehouse connection.

Requirements: pyspark>=3.5 (ships assertDataFrameEqual/assertSchemaEqual in
pyspark.testing.utils). Install locally with:
    pip install pyspark
CI already installs this - see .github/workflows/deploy.yml.
"""
import math
import os

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark is required for data-profiling unit tests")

from pyspark.sql import Row, SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual  # noqa: E402

from notebook_loader import load_notebook_definitions  # noqa: E402

NOTEBOOK_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "src",
    "notebooks",
    "data_profiling",
    "01_data_profiling.ipynb",
)

EXPECTED_SUMMARY_SCHEMA = StructType([
    StructField("Column Name", StringType(), False),
    StructField("Data Type", StringType(), False),
    StructField("Null Count", LongType(), False),
    StructField("Null Percentage", StringType(), False),
])


@pytest.fixture(scope="session")
def spark():
    """A single local SparkSession shared across this test module."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("unit-tests-01-data-profiling")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def captured_display():
    """
    Captures whatever DataFrame profile_dataset() passes to `display(...)`,
    so tests can assert on the summary table it produces (Databricks'
    built-in `display()` isn't available outside a notebook).
    """
    captured = {}

    def _fake_display(df):
        captured["df"] = df

    return captured, _fake_display


@pytest.fixture
def profile_dataset(spark, captured_display):
    """Loads the real profile_dataset() function from the notebook, wired to a local spark + fake display."""
    _, fake_display = captured_display
    ns = load_notebook_definitions(
        NOTEBOOK_PATH,
        injected_globals={"spark": spark, "display": fake_display},
    )
    return ns["profile_dataset"]


# ---------------------------------------------------------------------------
# Null counting / percentage correctness
# ---------------------------------------------------------------------------

def test_profile_dataset_counts_string_nulls_correctly(spark, profile_dataset, captured_display):
    """A STRING column with 1 NULL out of 4 rows must report Null Count=1, Null Percentage=25.00%."""
    captured, _ = captured_display
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ])
    df = spark.createDataFrame(
        [("1", "alice"), ("2", None), ("3", "carol"), ("4", "dave")],
        schema=schema,
    )

    profile_dataset(df, "unit-test-strings")
    summary = captured["df"]

    expected = spark.createDataFrame(
        [
            Row(**{"Column Name": "id", "Data Type": "string", "Null Count": 0, "Null Percentage": "0.00%"}),
            Row(**{"Column Name": "name", "Data Type": "string", "Null Count": 1, "Null Percentage": "25.00%"}),
        ],
        schema=EXPECTED_SUMMARY_SCHEMA,
    )
    assertDataFrameEqual(summary, expected)


def test_profile_dataset_treats_double_nan_as_null(spark, profile_dataset, captured_display):
    """
    profile_dataset() special-cases double/float columns to also count NaN as
    null (`isnan()` check in addition to `isNull()`), since NaN != NULL by
    default in Spark comparisons. A column with one real NULL and one NaN
    out of 4 rows must report Null Count=2.
    """
    captured, _ = captured_display
    schema = StructType([StructField("amount", DoubleType(), True)])
    df = spark.createDataFrame(
        [Row(amount=1.5), Row(amount=None), Row(amount=float("nan")), Row(amount=4.0)],
        schema=schema,
    )

    profile_dataset(df, "unit-test-nan")
    summary = captured["df"]

    null_count_row = summary.filter(summary["Column Name"] == "amount").collect()[0]
    assert null_count_row["Null Count"] == 2
    assert null_count_row["Null Percentage"] == "50.00%"


def test_profile_dataset_handles_empty_dataframe_without_division_error(spark, profile_dataset, captured_display):
    """With 0 rows, null percentage must default to '0.00%' instead of raising ZeroDivisionError."""
    captured, _ = captured_display
    schema = StructType([StructField("id", StringType(), True)])
    df = spark.createDataFrame([], schema=schema)

    profile_dataset(df, "unit-test-empty")
    summary = captured["df"]

    row = summary.collect()[0]
    assert row["Null Count"] == 0
    assert row["Null Percentage"] == "0.00%"


def test_profile_dataset_all_nulls_reports_100_percent(spark, profile_dataset, captured_display):
    """A fully-null column must report exactly 100.00%, not 99.99% or similar rounding drift."""
    captured, _ = captured_display
    schema = StructType([StructField("id", StringType(), True)])
    df = spark.createDataFrame([Row(id=None), Row(id=None)], schema=schema)

    profile_dataset(df, "unit-test-all-null")
    summary = captured["df"]

    row = summary.collect()[0]
    assert row["Null Count"] == 2
    assert row["Null Percentage"] == "100.00%"


def test_profile_dataset_column_with_space_in_name_is_escaped_safely(spark, profile_dataset, captured_display):
    """
    Real source columns contain spaces (e.g. 'Transaction unique identifier').
    profile_dataset() backtick-escapes column names when building null
    expressions - verify a spaced column name doesn't blow up the function.
    """
    captured, _ = captured_display
    schema = StructType([StructField("Transaction unique identifier", StringType(), True)])
    df = spark.createDataFrame([Row(**{"Transaction unique identifier": "TX1"}), Row(**{"Transaction unique identifier": None})], schema=schema)

    profile_dataset(df, "unit-test-spaced-column")
    summary = captured["df"]

    row = summary.filter(summary["Column Name"] == "Transaction unique identifier").collect()[0]
    assert row["Null Count"] == 1


# ---------------------------------------------------------------------------
# Output contract (schema)
# ---------------------------------------------------------------------------

def test_profile_dataset_summary_schema_matches_contract(spark, profile_dataset, captured_display):
    """The summary DataFrame's schema must exactly match the documented 4-column contract."""
    captured, _ = captured_display
    schema = StructType([StructField("col_a", StringType(), True)])
    df = spark.createDataFrame([Row(col_a="x")], schema=schema)

    profile_dataset(df, "unit-test-schema")
    summary = captured["df"]

    assertSchemaEqual(summary.schema, EXPECTED_SUMMARY_SCHEMA)


def test_profile_dataset_emits_one_summary_row_per_input_column(spark, profile_dataset, captured_display):
    """The summary table must have exactly as many rows as the input has columns."""
    captured, _ = captured_display
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True),
        StructField("c", DoubleType(), True),
    ])
    df = spark.createDataFrame([Row(a="1", b="2", c=3.0)], schema=schema)

    profile_dataset(df, "unit-test-row-count")
    summary = captured["df"]

    assert summary.count() == 3
    assert {r["Column Name"] for r in summary.collect()} == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# No-hardcoding / reusability check
# ---------------------------------------------------------------------------

def test_profile_dataset_is_reusable_across_datasets_with_different_schemas(spark, profile_dataset, captured_display):
    """
    profile_dataset(df, name) must work generically for ANY dataframe passed
    in (the Day-1 requirement is one reusable function covering all 4 chunk
    formats: CSV/CSV/JSON/XML) rather than being hardcoded to one schema.
    """
    captured, _ = captured_display

    csv_like = spark.createDataFrame([Row(price="100", city="London")], schema=StructType([
        StructField("price", StringType(), True),
        StructField("city", StringType(), True),
    ]))
    profile_dataset(csv_like, "csv-like")
    assert captured["df"].count() == 2

    json_like = spark.createDataFrame([Row(amount=1.0, qty=2.0, flag="Y")], schema=StructType([
        StructField("amount", DoubleType(), True),
        StructField("qty", DoubleType(), True),
        StructField("flag", StringType(), True),
    ]))
    profile_dataset(json_like, "json-like")
    assert captured["df"].count() == 3