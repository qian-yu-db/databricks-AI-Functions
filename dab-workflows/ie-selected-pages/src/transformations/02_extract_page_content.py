# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Page-Level Content from Parsed Documents
# MAGIC
# MAGIC This notebook extracts document content aggregated by pages instead of the whole document.
# MAGIC Each row represents a single page with its text content and image URI.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ie_selected_pages_workflow/02_extract_page_content",
    "Checkpoint location",
)
dbutils.widgets.text("source_table_name", "ie_parsed_documents_raw", "Source table name")
dbutils.widgets.text("table_name", "ie_page_content", "Output table name")

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

print("Starting Page-Level Content Extraction...")
print(f"Source Table: {catalog}.{schema}.{source_table_name}")
print(f"Output Table: {catalog}.{schema}.{table_name}")
print(f"Checkpoint: {checkpoint_location}")

from pyspark.sql.functions import col, expr, current_timestamp

# Read from source table using Structured Streaming
print("Reading from source table using Structured Streaming...")
parsed_stream = spark.readStream.format("delta").table(source_table_name)

# Extract page-level content from parsed documents using foreachBatch
# This allows us to use SQL for complex aggregations
print("Extracting page-level content from documents...")


def process_batch(batch_df, batch_id):
    """Process each micro-batch using SQL for proper aggregation"""
    if batch_df.isEmpty():
        return

    # Register the batch as a temp view
    batch_df.createOrReplaceTempView("batch_raw")

    # Use SQL to extract and aggregate page content (matching the working query pattern)
    page_content_sql = f"""
    WITH parsed_elements AS (
      SELECT
        path,
        parsed_at,
        cast(element:id as INT) as element_id,
        cast(element:type as STRING) as element_type,
        cast(element:bbox[0]:page_id as INT) as page_id,
        CASE
          WHEN cast(element:type as STRING) = 'figure' THEN cast(element:description as STRING)
          ELSE cast(element:content as STRING)
        END as content
      FROM (
        SELECT
          path,
          parsed_at,
          posexplode(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) AS (idx, element)
        FROM batch_raw
        WHERE
          parsed:document:elements IS NOT NULL
          AND CAST(parsed:error_status AS STRING) IS NULL
      )
    ),
    pages_info AS (
      SELECT
        path,
        cast(page:image_uri as STRING) as page_image_uri,
        page_idx as page_id
      FROM (
        SELECT
          path,
          posexplode(try_cast(parsed:document:pages AS ARRAY<VARIANT>)) AS (page_idx, page)
        FROM batch_raw
        WHERE parsed:document:pages IS NOT NULL
      )
    ),
    page_grouped AS (
      SELECT
        path,
        parsed_at,
        page_id,
        concat_ws('\\n\\n', collect_list(content)) AS page_content
      FROM (
        SELECT *
        FROM parsed_elements
        WHERE content IS NOT NULL AND trim(content) != ''
        ORDER BY path, page_id, element_id
      )
      GROUP BY path, parsed_at, page_id
    )
    SELECT
      pg.path,
      pg.page_id as page_number,
      pg.page_content,
      pi.page_image_uri,
      pg.parsed_at,
      current_timestamp() as extracted_at
    FROM page_grouped pg
    LEFT JOIN pages_info pi ON pg.path = pi.path AND pg.page_id = pi.page_id
    WHERE pg.page_content IS NOT NULL AND trim(pg.page_content) != ''
    """

    result_df = spark.sql(page_content_sql)

    # Append to target table
    result_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(table_name)


# Write using foreachBatch
print("Starting streaming write to Delta table...")
print("Processing mode: availableNow (batch processing)")
query = (
    parsed_stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("Page-level content extraction completed successfully!")
print(f"Results saved to: {catalog}.{schema}.{table_name}")
