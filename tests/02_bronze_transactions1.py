import os
import pytest
from databricks import sql

# Expected columns and types based on your notebook
EXPECTED_SCHEMA = {
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

@pytest.fixture
def cursor():
    """Connects to Databricks."""
    # Strip protocol prefix and trailing slash if present
    server_host = (
        os.environ["DATABRICKS_HOST"]
        .replace("https://", "")
        .replace("http://", "")
        .rstrip("/")
    )
    conn = sql.connect(
        server_hostname=server_host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        auth_type="databricks-oauth",
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )
    cur = conn.cursor()
    yield cur
    cur.close()
    conn.close()


# 1. Check if the table exists
def test_table_exists(cursor):
    cursor.execute("SHOW TABLES IN vstone.bronze LIKE 'transactions1'")
    result = cursor.fetchall()
    
    # If table exists, result will not be empty
    assert len(result) > 0, "Table vstone.bronze.transactions1 does NOT exist!"


# 2. Check if all columns exist and match expected data types
def test_columns_and_schema(cursor):
    cursor.execute("DESCRIBE TABLE vstone.bronze.transactions1")
    rows = cursor.fetchall()
    
    # Extract column names and types from the table
    # row[0] is column name, row[1] is data type
    actual_columns = {row[0]: row[1] for row in rows}

    for col_name, expected_type in EXPECTED_SCHEMA.items():
        # Check column presence
        assert col_name in actual_columns, f"Missing column: '{col_name}'"
        
        # Check column data type
        actual_type = actual_columns[col_name]
        assert actual_type == expected_type, (
            f"Column '{col_name}' has wrong type. Expected: {expected_type}, Got: {actual_type}"
        )