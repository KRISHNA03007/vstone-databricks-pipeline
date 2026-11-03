import pytest

VOLUME_PATH = "/Volumes/vstone/landing/raw_data/chunks"

EXPECTED_FILES = [
    "chunk1_initial.csv.gz",
    "chunk2_incremental.csv.gz",
    "chunk3.json.gz",
    "chunk4.xml.gz"
]

# Baseline core contract columns that must always exist
CORE_CONTRACT_COLUMNS = {
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


def test_all_volume_files_exist(cursor):
    """Verify that all 4 raw chunk files exist in the landing Volume."""
    cursor.execute(f"LIST '{VOLUME_PATH}'")
    rows = cursor.fetchall()
    
    volume_files = " ".join([str(row[0]) for row in rows])
    
    for filename in EXPECTED_FILES:
        assert filename in volume_files, f"Missing file in volume {VOLUME_PATH}: {filename}"


def test_chunk1_dynamic_schema_contract(cursor):
    """
    Dynamically inspect columns in chunk1_initial.csv.gz via read_files metadata.
    Handles schema evolution by verifying core contract columns exist as a subset,
    allowing additive evolution without breaking on column count changes.
    """
    cursor.execute(f"""
        SELECT * FROM read_files(
            '{VOLUME_PATH}/chunk1_initial.csv.gz',
            format => 'csv',
            header => true
        ) LIMIT 0
    """)
    
    # Dynamically extract column names directly from source file metadata
    actual_columns = {col[0] for col in cursor.description}
    
    missing_core = CORE_CONTRACT_COLUMNS - actual_columns
    assert not missing_core, (
        f"Schema contract violation in chunk1: missing required columns: {missing_core}"
    )


def test_schema_evolution_compatibility_across_chunks(cursor):
    """
    Dynamically compares schemas between initial and incremental chunks to ensure
    schema evolution is backward-compatible and additive without dropping core columns.
    """
    # 1. Dynamically read Chunk 1 columns
    cursor.execute(f"""
        SELECT * FROM read_files(
            '{VOLUME_PATH}/chunk1_initial.csv.gz',
            format => 'csv',
            header => true
        ) LIMIT 0
    """)
    chunk1_cols = {col[0] for col in cursor.description}

    # 2. Dynamically read Chunk 2 columns
    cursor.execute(f"""
        SELECT * FROM read_files(
            '{VOLUME_PATH}/chunk2_incremental.csv.gz',
            format => 'csv',
            header => true
        ) LIMIT 0
    """)
    chunk2_cols = {col[0] for col in cursor.description}

    # Verify primary key is preserved across evolved chunks
    assert "Transaction unique identifier" in chunk1_cols, "Chunk 1 missing primary key"
    assert "Transaction unique identifier" in chunk2_cols, "Chunk 2 missing primary key"

    # Verify no core business columns were dropped during evolution
    dropped_core = CORE_CONTRACT_COLUMNS - chunk2_cols
    assert not dropped_core, f"Incremental Chunk 2 dropped core contract columns: {dropped_core}"


def test_chunk1_not_empty(cursor):
    """Verify that chunk1_initial.csv.gz contains data records."""
    cursor.execute(f"""
        SELECT COUNT(*) FROM read_files(
            '{VOLUME_PATH}/chunk1_initial.csv.gz',
            format => 'csv',
            header => true
        )
    """)
    row_count = cursor.fetchone()[0]
    assert row_count > 0, "chunk1_initial.csv.gz is empty!"


def test_chunk1_primary_key_not_null(cursor):
    """Verify that the primary key column has no NULL values in chunk1 sample."""
    cursor.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT `Transaction unique identifier`
            FROM read_files(
                '{VOLUME_PATH}/chunk1_initial.csv.gz',
                format => 'csv',
                header => true
            )
            LIMIT 1000
        )
        WHERE `Transaction unique identifier` IS NULL
    """)
    null_count = cursor.fetchone()[0]
    assert null_count == 0, f"Found {null_count} nulls in 'Transaction unique identifier' in chunk1 sample"
