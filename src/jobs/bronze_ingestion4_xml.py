resources:
  jobs:
    bronze_ingestion_chunk4:
      name: "Day 3 - Bronze Layer Ingestion (Chunk 4 - XML via PySpark)"
      tasks:
        - task_key: "read_write_bronze_chunk4"
          notebook_task:
            notebook_path: ../notebooks/bronze/04_chunk4_xml_pyspark.py
            base_parameters:
              source_path: "${var.chunk4_source_path}"
              catalog: "${var.catalog}"
              schema: "${var.bronze_schema}"
              table_name: "${var.chunk4_table_name}"
              row_tag: "${var.chunk4_row_tag}"