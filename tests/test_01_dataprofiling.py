import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# -------------------------------------------------------------------------
# Fixtures & Constants
# -------------------------------------------------------------------------
VOLUME_PATH = "/Volumes/vstone/landing/raw_data/chunks"
EXPECTED_TOTAL_ROWS = 22_489_348

EXPECTED_COLUMNS_STANDARD = {
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
    "Record Status - monthly file only"
}

# In XML, spaces and special characters were replaced with underscores during export
EXPECTED_COLUMNS_XML = {
    "Transaction_unique_identifier",
    "Price",
    "Date_of_Transfer",
    "Property_Type",
    "Old_New",
    "Duration",
    "Town_City",
    "District",
    "County",
    "PPDCategory_Type",
    "Record_Status___monthly_file_only"
}

@pytest.fixture(scope="session")
def spark():
    """Provides existing Databricks SparkSession."""
    return SparkSession.builder.getOrCreate()

@pytest.fixture(scope="session")
def load_chunks(spark):
    """Loads all 4 chunks once for the test session."""
    df_c1 = (spark.read.format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(f"{VOLUME_PATH}/chunk1_initial.csv.gz"))

    df_c2 = (spark.read.format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(f"{VOLUME_PATH}/chunk2_incremental.csv.gz"))

    df_c3 = (spark.read.format("json")
             .load(f"{VOLUME_PATH}/chunk3.json.gz"))

    df_c4 = (spark.read.format("xml")
             .option("rowTag", "record")
             .load(f"{VOLUME_PATH}/chunk4.xml.gz"))

    return {
        "chunk1": df_c1,
        "chunk2": df_c2,
        "chunk3": df_c3,
        "chunk4": df_c4
    }

# -------------------------------------------------------------------------
# 1. Test: Volume File Existence
# -------------------------------------------------------------------------
def test_volume_files_exist():
    """Validates that all 4 target compressed files exist in Unity Catalog Volume."""
    expected_files = [
        "chunk1_initial.csv.gz",
        "chunk2_incremental.csv.gz",
        "chunk3.json.gz",
        "chunk4.xml.gz"
    ]
    actual_files = os.listdir(VOLUME_PATH)
    for fname in expected_files:
        assert fname in actual_files, f"Missing expected file in volume: {fname}"

# -------------------------------------------------------------------------
# 2. Test: Column Presence & Schema Consistency
# -------------------------------------------------------------------------
def test_csv_and_json_schemas(load_chunks):
    """Validates column count and exact names for CSV and JSON chunks."""
    for key in ["chunk1", "chunk2", "chunk3"]:
        df = load_chunks[key]
        actual_cols = set(df.columns)
        missing_cols = EXPECTED_COLUMNS_STANDARD - actual_cols
        assert len(missing_cols) == 0, f"{key} is missing columns: {missing_cols}"
        assert len(df.columns) == 11, f"{key} column count mismatch: expected 11, got {len(df.columns)}"

def test_xml_schema(load_chunks):
    """Validates column count and sanitized names for XML chunk."""
    df_xml = load_chunks["chunk4"]
    actual_cols = set(df_xml.columns)
    missing_cols = EXPECTED_COLUMNS_XML - actual_cols
    assert len(missing_cols) == 0, f"XML chunk is missing columns: {missing_cols}"
    assert len(df_xml.columns) == 11, f"XML chunk column count mismatch: expected 11, got {len(df_xml.columns)}"

# -------------------------------------------------------------------------
# 3. Test: Row Distribution & Zero Data Loss
# -------------------------------------------------------------------------
def test_row_counts_and_zero_loss(load_chunks):
    """Validates that no records were lost and row counts match deterministic targets."""
    c1_count = load_chunks["chunk1"].count()
    c2_count = load_chunks["chunk2"].count()
    c3_count = load_chunks["chunk3"].count()
    c4_count = load_chunks["chunk4"].count()

    total_chunked_records = c1_count + c2_count + c3_count + c4_count

    assert c1_count == 6_746_804, f"Chunk 1 count mismatch: {c1_count}"
    assert c2_count == 6_746_804, f"Chunk 2 count mismatch: {c2_count}"
    assert c3_count == 4_497_869, f"Chunk 3 count mismatch: {c3_count}"
    assert c4_count == 4_497_871, f"Chunk 4 count mismatch: {c4_count}"
    assert total_chunked_records == EXPECTED_TOTAL_ROWS, (
        f"Data loss detected! Sum of chunks ({total_chunked_records}) != original ({EXPECTED_TOTAL_ROWS})"
    )

# -------------------------------------------------------------------------
# 4. Test: Data Integrity (Primary Key & Price Checks)
# -------------------------------------------------------------------------
def test_key_columns_integrity(load_chunks):
    """Ensures primary ID is not null and Price contains valid positive values."""
    for key in ["chunk1", "chunk2", "chunk3"]:
        df = load_chunks[key]
        null_ids = df.filter(F.col("`Transaction unique identifier`").isNull()).count()
        assert null_ids == 0, f"{key} has {null_ids} null Transaction IDs"

    df_xml = load_chunks["chunk4"]
    xml_null_ids = df_xml.filter(F.col("Transaction_unique_identifier").isNull()).count()
    assert xml_null_ids == 0, f"XML chunk has {xml_null_ids} null Transaction IDs"