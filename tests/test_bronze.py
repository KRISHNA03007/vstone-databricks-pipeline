import unittest
import os

from pyspark.sql import SparkSession


class TestBronzeLayer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.spark = SparkSession.builder.getOrCreate()

        # Passed separately through run_tests.py
        cls.source_path = os.environ["SOURCE_PATH"]
        cls.table_name = os.environ["TABLE_NAME"]
        cls.file_format = os.environ.get("FILE_FORMAT", "csv")
        cls.row_tag = os.environ.get("ROW_TAG")

        # Read source data from Unity Catalog Volume
        reader = cls.spark.read

        if cls.file_format == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")

        if cls.file_format == "xml" and cls.row_tag:
            reader = reader.option("rowTag", cls.row_tag)

        cls.source_df = reader.format(cls.file_format).load(cls.source_path)

        # Read Bronze table
        cls.bronze_df = cls.spark.table(cls.table_name)

    # 1. Check whether Bronze table exists
    def test_table_exists(self):

        self.assertTrue(
            self.spark.catalog.tableExists(self.table_name),
            f"Bronze table does not exist: {self.table_name}"
        )

    # 2. Check whether columns match
    def test_columns_match(self):

        source_columns = self.source_df.columns

        bronze_columns = [
            col for col in self.bronze_df.columns
            if col not in ["load_dt", "source"]
        ]

        self.assertEqual(
            source_columns,
            bronze_columns,
            "Source and Bronze columns are not matching"
        )

    # 3. Check whether records match
    def test_records_match(self):

        source_columns = self.source_df.columns

        bronze_data = self.bronze_df.select(*source_columns)

        source_count = self.source_df.count()
        bronze_count = bronze_data.count()

        self.assertEqual(
            source_count,
            bronze_count,
            "Source and Bronze record counts are different"
        )

        # Compare actual records, including duplicate records
        source_diff = self.source_df.exceptAll(bronze_data).limit(1).count()

        bronze_diff = bronze_data.exceptAll(self.source_df).limit(1).count()

        self.assertEqual(
            source_diff + bronze_diff,
            0,
            "Source and Bronze records are not matching"
        )