import pytest

EXPECTED_FILES = [
    "chunk1_initial.csv.gz",
    "chunk2_incremental.csv.gz",
    "chunk3.json.gz",
    "chunk4.xml.gz"
]

EXPECTED_COLUMNS = [
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
]

# 1. Check if all 4 volume files exist
def test_all_volume_files_exist(cursor):
    cursor.execute("LIST '/Volumes/vstone/landing/raw_data/chunks'")
    rows = cursor.fetchall()
    
    # Extract file paths/names returned by Databricks LIST command
    volume_files = " ".join([str(row[0]) for row in rows])
    
    for filename in EXPECTED_FILES:
        assert filename in volume_files, f"Missing file in volume: {filename}"


# 2. Check schema and column presence in the initial raw CSV file
def test_chunk1_columns(cursor):
    cursor.execute("""
        SELECT * FROM read_files(
            '/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz',
            format => 'csv',
            header => true
        ) LIMIT 1
    """)
    
    # Read actual column names from query metadata
    actual_columns = [col[0] for col in cursor.description]
    
    assert len(actual_columns) == 11, f"Expected 11 columns, got {len(actual_columns)}"
    
    for col_name in EXPECTED_COLUMNS:
        assert col_name in actual_columns, f"Missing column: {col_name}"