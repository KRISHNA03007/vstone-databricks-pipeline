import os
import pytest
from databricks import sql

@pytest.fixture(scope="session")
def db_cursor():
    """Establishes connection to Databricks Unity Catalog using CI/CD environment variables."""
    # Strip protocol prefix if present
    server_hostname = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "")
    
    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        auth_type="databricks-oauth",
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )
    cursor = connection.cursor()
    yield cursor
    cursor.close()
    connection.close()