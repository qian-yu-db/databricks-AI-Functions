# Databricks notebook source
# MAGIC %md
# MAGIC # Classify Pages Using ai_query
# MAGIC
# MAGIC This notebook classifies pages using ai_query based on a configurable prompt.
# MAGIC Binary classification: page contains certain info or not.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ie_selected_pages_workflow/03_classify_pages",
    "Checkpoint location",
)
dbutils.widgets.text("source_table_name", "ie_page_content", "Source table name")
dbutils.widgets.text("table_name", "ie_classified_pages", "Output table name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
source_table_name = dbutils.widgets.get("source_table_name")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import yaml

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

LLM_MODEL = config.get("LLM_MODEL", "databricks-claude-sonnet-4")
POSITIVE_LABEL = config.get("POSITIVE_LABEL", "contains_target_info")
NEGATIVE_LABEL = config.get("NEGATIVE_LABEL", "no_target_info")
CLASSIFICATION_PROMPT = config.get("CLASSIFICATION_PROMPT", "Classify the following document:")

# Escape single quotes in the prompt for SQL
CLASSIFICATION_PROMPT_ESCAPED = CLASSIFICATION_PROMPT.replace("'", "''")

print("Starting Page Classification...")
print(f"Source Table: {catalog}.{schema}.{source_table_name}")
print(f"Output Table: {catalog}.{schema}.{table_name}")
print(f"Checkpoint: {checkpoint_location}")
print(f"LLM Model: {LLM_MODEL}")
print(f"Classification Labels: [{POSITIVE_LABEL}, {NEGATIVE_LABEL}]")

# COMMAND ----------

from pyspark.sql.functions import col, expr, current_timestamp

# Read from source table using Structured Streaming
print("Reading from source table using Structured Streaming...")
page_content_stream = (
    spark.readStream.format("delta")
    .table(source_table_name)
    .filter(col("page_content").isNotNull())
)

# Classify pages using ai_query with the classification prompt
print("Classifying pages using ai_query...")

classified_df = (
    page_content_stream
    .withColumn(
        "classification_result",
        expr(f"""
            ai_query(
                '{LLM_MODEL}',
                concat('{CLASSIFICATION_PROMPT_ESCAPED}', page_content),
                modelParameters => named_struct(
                    'max_tokens', 100,
                    'temperature', 0.0
                )
            )
        """)
    )
    # Extract the classification label from the result
    # The result should contain one of the labels
    .withColumn(
        "classification_label",
        expr(f"""
            CASE
                WHEN classification_result LIKE '%{POSITIVE_LABEL}%' THEN '{POSITIVE_LABEL}'
                ELSE '{NEGATIVE_LABEL}'
            END
        """)
    )
    .withColumn(
        "is_target_page",
        expr(f"classification_label = '{POSITIVE_LABEL}'")
    )
    .withColumn("classified_at", current_timestamp())
    .select(
        "path",
        "page_number",
        "page_content",
        "page_image_uri",
        "classification_label",
        "is_target_page",
        "parsed_at",
        "extracted_at",
        "classified_at",
    )
)

# Write to Delta table with streaming
print("Starting streaming write to Delta table...")
print("Processing mode: availableNow (batch processing)")
query = (
    classified_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("Page classification completed successfully!")
print(f"Results saved to: {catalog}.{schema}.{table_name}")
