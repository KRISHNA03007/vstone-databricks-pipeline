# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit


def get_parameters():
    dbutils.widgets.text("source_path", "")
    dbutils.widgets.text("catalog", "vstone")
    dbutils.widgets.text("schema", "bronze")
    dbutils.widgets.text("table_name", "transactions3")
    dbutils.widgets.text("checkpoint_path", "")
    return {
        "source_path": dbutils.widgets.get("source_path"),
        "catalog": dbutils.widgets.get("catalog"),
        "schema": dbutils.widgets.get("schema"),
        "table_name": dbutils.widgets.get("table_name"),
        "checkpoint_path": dbutils.widgets.get("checkpoint_path"),
    }


def ensure_schema_exists(catalog, schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def ingest_json_autoloader(target_table, source_path, checkpoint_path):
    # Auto Loader: incremental, idempotent by design (tracks processed files
    # in the checkpoint). trigger(availableNow=True) processes what's new,
    # then stops - safe to re-run for "multiple run scenarios".
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .load(source_path)
    )
    df = df.withColumn("load_dt", current_timestamp()).withColumn("source", lit(source_path))

    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
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
    ingest_json_autoloader(target_table, params["source_path"], params["checkpoint_path"])