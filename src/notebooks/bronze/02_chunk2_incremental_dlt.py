# Databricks notebook source
# Delta Live Tables pipeline: Bronze ingestion for chunk2 (incremental CSV).
# Values below come from the pipeline's "configuration" block in
# src/pipelines/bronze_ingestion2_dlt.yml - nothing is hardcoded here.

import dlt
from pyspark.sql.functions import current_timestamp, lit

source_path = spark.conf.get("source_path")
table_name = spark.conf.get("table_name")


@dlt.table(
    name=table_name,
    comment="Bronze transactions table loaded via Delta Live Tables from chunk2 incremental CSV",
)
def bronze_chunk2():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
    )
    return df.withColumn("load_dt", current_timestamp()).withColumn("source", lit(source_path))