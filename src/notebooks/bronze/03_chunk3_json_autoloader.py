# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit


def get_parameters():
    dbutils.widgets.text("source_path", "")
    dbutils.widgets.text("file_pattern", "")
    dbutils.widgets.text("catalog", "vstone")
    dbutils.widgets.text("schema", "bronze")
    dbutils.widgets.text("table_name", "transactions3")
    dbutils.widgets.text("checkpoint_path", "")
    return {
        "source_path": dbutils.widgets.get("source_path"),
        "file_pattern": dbutils.widgets.get("file_pattern"),
        "catalog": dbutils.widgets.get("catalog"),
        "schema": dbutils.widgets.get("schema"),
        "table_name": dbutils.widgets.get("table_name"),
        "checkpoint_path": dbutils.widgets.get("checkpoint_path"),
    }


def ensure_schema_exists(catalog, schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def ingest_json_autoloader(target_table, source_path, file_pattern, checkpoint_path):
    # Auto Loader watches a folder, not a single file. chunk1/2/4 files live
    # in the same folder, so pathGlobFilter keeps this job to only the
    # chunk3 file. Idempotent by design (tracks processed files in the
    # checkpoint); trigger(availableNow=True) processes what's new then stops.
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("pathGlobFilter", file_pattern)
        .load(source_path)
    )
    df = df.withColumn("load_dt", current_timestamp()).withColumn("source", lit(source_path))

    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        # Source columns may contain spaces (e.g. "Transaction unique
        # identifier"), same as chunk1/chunk2 - column mapping allows that.
        .option("delta.columnMapping.mode", "name")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()

    spark.sql(
        f"COMMENT ON TABLE {target_table} IS "
        "'Bronze transactions table loaded from chunk3 JSON via Auto Loader'"
    )


# COMMAND ----------
if __name__ == "__main__":
    params = get_parameters()
    target_table = f"{params['catalog']}.{params['schema']}.{params['table_name']}"

    ensure_schema_exists(params["catalog"], params["schema"])
    ingest_json_autoloader(
        target_table,
        params["source_path"],
        params["file_pattern"],
        params["checkpoint_path"],
    )