import os
import pytest
from databricks import sql

REQUIRED_ENV_VARS = [
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
]

@pytest.fixture(scope="session")
def db_cursor():
    """Establishes a shared connection to Databricks Unity Catalog using CI/CD environment variables."""
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing_vars:
        pytest.fail(f"Missing required Databricks CI/CD environment variables: {', '.join(missing_vars)}")

    server_hostname = (
        os.environ["DATABRICKS_HOST"]
        .replace("https://", "")
        .replace("http://", "")
        .rstrip("/")
    )

    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        auth_type="databricks-oauth",
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        connection.close()

@pytest.fixture(scope="session")
def cursor(db_cursor):
    """Alias for db_cursor so tests can use either 'cursor' or 'db_cursor' as a fixture."""
    return db_cursor
