# Databricks notebook source
# Delta Live Tables pipeline: Bronze ingestion for chunk2 (incremental CSV).
# Values below come from the pipeline's "configuration" block in
# src/pipelines/bronze_ingestion2_dlt.yml - nothing is hardcoded here.

import re
import dlt
from pyspark.sql.functions import current_timestamp, lit

source_path = spark.conf.get("source_path")
table_name = spark.conf.get("table_name")


def clean_column_name(name):
    # Materialized Views (what a static @dlt.table read becomes) don't
    # support Delta column mapping, so spaces/special characters in source
    # columns (e.g. "Transaction unique identifier") must be replaced.
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_")


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
    df = df.toDF(*[clean_column_name(c) for c in df.columns])
    return df.withColumn("load_dt", current_timestamp()).withColumn("source", lit(source_path))