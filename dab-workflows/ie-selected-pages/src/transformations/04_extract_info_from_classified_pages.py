# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Key Information from Classified Pages
# MAGIC
# MAGIC This notebook extracts key information from pages that have been classified as containing target information.
# MAGIC Uses ai_query to extract structured data based on a configurable prompt.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ie_selected_pages_workflow/04_extract_info_from_classified_pages",
    "Checkpoint location",
)
dbutils.widgets.text("source_table_name", "ie_classified_pages", "Source table name")
dbutils.widgets.text("table_name", "ie_extracted_info", "Output table name")

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
EXTRACTION_PROMPT = config.get("EXTRACTION_PROMPT", "Extract key information from the following text:")
RESPONSE_FORMAT = config.get("RESPONSE_FORMAT", "")
POSITIVE_LABEL = config.get("POSITIVE_LABEL", "contains_target_info")

# Escape single quotes in prompts for SQL
EXTRACTION_PROMPT_ESCAPED = EXTRACTION_PROMPT.replace("'", "''")

# Clean and escape RESPONSE_FORMAT for SQL (remove extra whitespace and escape quotes)
if RESPONSE_FORMAT:
    import json
    # Parse and re-serialize to ensure valid JSON and remove extra whitespace
    RESPONSE_FORMAT_CLEANED = json.dumps(json.loads(RESPONSE_FORMAT))
    # Escape single quotes for SQL
    RESPONSE_FORMAT_ESCAPED = RESPONSE_FORMAT_CLEANED.replace("'", "''")
else:
    RESPONSE_FORMAT_ESCAPED = ""

print("Starting Key Information Extraction...")
print(f"Source Table: {catalog}.{schema}.{source_table_name}")
print(f"Output Table: {catalog}.{schema}.{table_name}")
print(f"Checkpoint: {checkpoint_location}")
print(f"LLM Model: {LLM_MODEL}")
print(f"Filtering for label: {POSITIVE_LABEL}")

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, expr, lit

# Read from source table using Structured Streaming
# Only process pages classified as containing target information
print("Reading from source table using Structured Streaming...")
print("Filtering to only process pages with target classification...")

classified_stream = (
    spark.readStream.format("delta")
    .table(source_table_name)
    .filter(col("classification_label") == POSITIVE_LABEL)
    .filter(col("page_content").isNotNull())
)

# Extract structured information using ai_query
print("Extracting key information using ai_query...")

# Build the ai_query expression
if RESPONSE_FORMAT_ESCAPED:
    print(f"Using responseFormat for structured output")
    ai_query_expr = f"""
        ai_query(
            '{LLM_MODEL}',
            concat('{EXTRACTION_PROMPT_ESCAPED}', page_content),
            responseFormat => '{RESPONSE_FORMAT_ESCAPED}',
            modelParameters => named_struct(
                'max_tokens', 5000,
                'temperature', 0.0
            )
        )
    """
else:
    print("No responseFormat specified, using free-form output")
    ai_query_expr = f"""
        ai_query(
            '{LLM_MODEL}',
            concat('{EXTRACTION_PROMPT_ESCAPED}', page_content),
            modelParameters => named_struct(
                'max_tokens', 5000,
                'temperature', 0.0
            )
        )
    """

extracted_df = (
    classified_stream
    .withColumn(
        "extracted_info",
        expr(ai_query_expr)
    )
    .withColumn("extraction_timestamp", current_timestamp())
    .select(
        "path",
        "page_number",
        "page_content",
        "page_image_uri",
        "classification_label",
        "extracted_info",
        "parsed_at",
        "extracted_at",
        "classified_at",
        "extraction_timestamp",
    )
)

# Write to Delta table with streaming
print("Starting streaming write to Delta table...")
print("Processing mode: availableNow (batch processing)")
query = (
    extracted_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("Key information extraction completed successfully!")
print(f"Results saved to: {catalog}.{schema}.{table_name}")
