import pytest

TARGET_CATALOG = "vstone"
TARGET_SCHEMA = "bronze"
TARGET_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.transactions1"
SOURCE_FILE = "/Volumes/vstone/landing/raw_data/chunks/chunk1_initial.csv.gz"

# Core business contract columns and expected data types (required baseline)
CORE_CONTRACT_SCHEMA = {
    "Transaction unique identifier": "string",
    "Price": "string",
    "Date of Transfer": "string",
    "Property Type": "string",
    "Old/New": "string",
    "Duration": "string",
    "Town/City": "string",
    "District": "string",
    "County": "string",
    "PPDCategory Type": "string",
    "Record Status - monthly file only": "string",
    "load_dt": "timestamp",
    "source": "string"
}


def test_schema_exists(cursor):
    """Verify that target catalog schema (vstone.bronze) exists."""
    cursor.execute(f"SHOW SCHEMAS IN {TARGET_CATALOG} LIKE '{TARGET_SCHEMA}'")
    schemas = cursor.fetchall()
    assert len(schemas) > 0, f"Schema '{TARGET_CATALOG}.{TARGET_SCHEMA}' does NOT exist!"


def test_table_exists(cursor):
    """Verify that Bronze table (vstone.bronze.transactions1) exists."""
    cursor.execute(f"SHOW TABLES IN {TARGET_CATALOG}.{TARGET_SCHEMA} LIKE 'transactions1'")
    tables = cursor.fetchall()
    assert len(tables) > 0, f"Table '{TARGET_TABLE}' does NOT exist!"


def test_table_schema_evolution_properties(cursor):
    """
    Verify table properties required for Delta schema evolution:
    - delta.columnMapping.mode = 'name' enables column mapping, spaces in column names,
      and non-blocking schema evolution.
    """
    cursor.execute(f"SHOW TBLPROPERTIES {TARGET_TABLE}")
    rows = cursor.fetchall()
    tbl_properties = {row[0].strip(): str(row[1]).strip() for row in rows}

    mapping_mode = tbl_properties.get("delta.columnMapping.mode")
    assert mapping_mode == "name", (
        f"delta.columnMapping.mode is '{mapping_mode}', expected 'name' on '{TARGET_TABLE}'. "
        "Column mapping mode 'name' is required to support schema evolution and column spaces."
    )


def test_dynamic_source_to_target_schema_evolution(cursor):
    """
    Dynamic Schema Evolution Test:
    Infers the source file schema dynamically using read_files metadata and validates
    that EVERY source column has been merged into the target Delta table.
    Eliminates rigid hardcoded column lengths and verifies that mergeSchema = 'true'
    accommodates all incoming columns.
    """
    # 1. Dynamically infer schema from incoming source file
    cursor.execute(f"""
        SELECT * FROM read_files(
            '{SOURCE_FILE}',
            format => 'csv',
            header => true
        ) LIMIT 0
    """)
    source_columns = {col[0] for col in cursor.description}

    # 2. Dynamically fetch current Bronze table schema
    cursor.execute(f"DESCRIBE TABLE {TARGET_TABLE}")
    table_rows = cursor.fetchall()
    target_columns = {
        row[0].strip()
        for row in table_rows
        if row[0] and not row[0].startswith("#")
    }

    # 3. Validate that target schema is a superset of source schema
    missing_in_target = source_columns - target_columns
    assert not missing_in_target, (
        f"Schema evolution failed! Target table '{TARGET_TABLE}' is missing evolved columns from source: {missing_in_target}"
    )


def test_core_contract_and_types(cursor):
    """
    Verify that core contract columns and audit fields exist with their expected data types,
    while permitting additive schema evolution (new columns added by COPY INTO mergeSchema).
    """
    cursor.execute(f"DESCRIBE TABLE {TARGET_TABLE}")
    rows = cursor.fetchall()

    actual_columns = {
        row[0].strip(): row[1].strip().lower()
        for row in rows
        if row[0] and not row[0].startswith("#")
    }

    # Verify baseline contract columns exist with matching types
    for col_name, expected_type in CORE_CONTRACT_SCHEMA.items():
        assert col_name in actual_columns, (
            f"Core contract violation: Missing column '{col_name}' in '{TARGET_TABLE}'"
        )
        actual_type = actual_columns[col_name]
        assert actual_type == expected_type.lower(), (
            f"Type mismatch on '{col_name}' in '{TARGET_TABLE}'. "
            f"Expected: {expected_type}, Got: {actual_type}"
        )


def test_table_is_delta_format(cursor):
    """Verify that the table format is DELTA."""
    cursor.execute(f"DESCRIBE DETAIL {TARGET_TABLE}")
    row = cursor.fetchone()
    columns = [col[0].lower() for col in cursor.description]
    format_idx = columns.index("format") if "format" in columns else 1
    table_format = str(row[format_idx]).lower()
    assert table_format == "delta", f"Table '{TARGET_TABLE}' format is '{table_format}', expected 'delta'"


def test_records_ingested(cursor):
    """Verify that records have been ingested into Bronze table via COPY INTO."""
    cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
    count = cursor.fetchone()[0]
    assert count > 0, f"Table '{TARGET_TABLE}' has 0 records; COPY INTO ingestion may have failed!"


def test_audit_columns_populated(cursor):
    """Verify that audit columns (load_dt, source) are populated and not NULL."""
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {TARGET_TABLE}
        WHERE load_dt IS NULL OR source IS NULL
    """)
    null_audit_count = cursor.fetchone()[0]
    assert null_audit_count == 0, (
        f"Found {null_audit_count} records in '{TARGET_TABLE}' with NULL audit columns (load_dt / source)!"
    )


def test_primary_key_integrity(cursor):
    """Verify that 'Transaction unique identifier' does not contain NULL values."""
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {TARGET_TABLE}
        WHERE `Transaction unique identifier` IS NULL
    """)
    null_pk_count = cursor.fetchone()[0]
    assert null_pk_count == 0, (
        f"Found {null_pk_count} records with NULL 'Transaction unique identifier' in '{TARGET_TABLE}'!"
    )
