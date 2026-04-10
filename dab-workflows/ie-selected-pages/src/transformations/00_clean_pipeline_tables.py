# Databricks notebook source
# MAGIC %md
# MAGIC # Clean Pipeline Tables and Checkpoints
# MAGIC
# MAGIC This notebook provides a utility to clean up all tables and checkpoints created by the IE selected pages workflow.
# MAGIC
# MAGIC ## What Gets Cleaned
# MAGIC
# MAGIC ### Tables (4 total):
# MAGIC 1. **ie_parsed_documents_raw** - Raw parsed documents from ai_parse_document
# MAGIC 2. **ie_page_content** - Extracted page-level content
# MAGIC 3. **ie_classified_pages** - Pages classified using ai_classify
# MAGIC 4. **ie_extracted_info** - Extracted key information from classified pages
# MAGIC
# MAGIC ### Checkpoint Directories (4 total):
# MAGIC - 01_parse_documents
# MAGIC - 02_extract_page_content
# MAGIC - 03_classify_pages
# MAGIC - 04_extract_info_from_classified_pages
# MAGIC
# MAGIC ## Usage
# MAGIC Set the widget **"Clean all pipeline tables and checkpoints?"** to **"Yes"** to execute cleanup.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "output_volume_path",
    "/Volumes/fins_genai/unstructured_documents/ie_selected_pages_workflow/outputs/",
    "Output volume path",
)
dbutils.widgets.dropdown(
    name="clean_pipeline_tables",
    choices=["No", "Yes"],
    defaultValue="No",
    label="Clean all pipeline tables and checkpoints?",
)
dbutils.widgets.text("raw_table_name", "ie_parsed_documents_raw", "Raw table name")
dbutils.widgets.text(
    "page_content_table_name", "ie_page_content", "Page content table name"
)
dbutils.widgets.text(
    "classified_pages_table_name", "ie_classified_pages", "Classified pages table name"
)
dbutils.widgets.text(
    "extracted_info_table_name", "ie_extracted_info", "Extracted info table name"
)
dbutils.widgets.text(
    "checkpoint_base_path",
    "checkpoints/ie_selected_pages_workflow",
    "Checkpoint base path",
)
dbutils.widgets.text(
    "volume_name",
    "ie_selected_pages_workflow",
    "Unity Catalog volume name",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
output_volume_path = dbutils.widgets.get("output_volume_path")
clean_pipeline = dbutils.widgets.get("clean_pipeline_tables")
raw_table_name = dbutils.widgets.get("raw_table_name")
page_content_table_name = dbutils.widgets.get("page_content_table_name")
classified_pages_table_name = dbutils.widgets.get("classified_pages_table_name")
extracted_info_table_name = dbutils.widgets.get("extracted_info_table_name")
checkpoint_base_path = dbutils.widgets.get("checkpoint_base_path")
volume_name = dbutils.widgets.get("volume_name")

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import shutil
import os


def normalize_path(path):
    """Normalize a path by removing trailing slashes and double slashes"""
    # Remove trailing slashes
    path = path.rstrip("/")
    # Replace double slashes with single (except for the protocol part)
    while "//" in path:
        path = path.replace("//", "/")
    return path


def get_checkpoint_full_path(checkpoint_subpath):
    """Get the full path for a checkpoint, handling both absolute and relative paths"""
    checkpoint_subpath = normalize_path(checkpoint_subpath)
    if checkpoint_subpath.startswith("/Volumes/"):
        return checkpoint_subpath
    else:
        return f"/Volumes/{catalog}/{schema}/{checkpoint_subpath}"


def ensure_volume_exists(vol_name):
    """Ensure a Unity Catalog volume exists, create if it doesn't"""
    if not vol_name or vol_name.strip() == "":
        print(f"  Skipping empty volume name")
        return False
    try:
        spark.sql(f"DESCRIBE VOLUME {catalog}.{schema}.{vol_name}")
        print(f"  Volume already exists: {catalog}.{schema}.{vol_name}")
        return True
    except Exception:
        try:
            spark.sql(
                f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{vol_name}"
            )
            print(f"  Created volume: {catalog}.{schema}.{vol_name}")
            return True
        except Exception as e:
            print(
                f"  Failed to create volume {catalog}.{schema}.{vol_name}: {str(e)}"
            )
            return False


def ensure_directories_exist():
    """Ensure required volumes and directories exist, create if they don't"""

    print("Checking and creating required volumes and directories...")

    # Ensure the main workflow volume exists
    print("\nChecking Unity Catalog volumes...")
    ensure_volume_exists(volume_name)

    # Get the checkpoint directory full path
    checkpoint_full_path = get_checkpoint_full_path(checkpoint_base_path)

    print("\nCreating subdirectories...")
    required_dirs = [
        normalize_path(output_volume_path),
        checkpoint_full_path,
    ]

    for directory in required_dirs:
        try:
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                print(f"  Created directory: {directory}")
            else:
                print(f"  Directory already exists: {directory}")
        except Exception as e:
            print(f"  Failed to create directory {directory}: {str(e)}")

    print("\nVolume and directory check completed!\n")


def cleanup_pipeline():
    """Clean up all pipeline tables and checkpoints"""

    print("Starting pipeline cleanup...")

    tables_to_drop = [
        raw_table_name,
        page_content_table_name,
        classified_pages_table_name,
        extracted_info_table_name,
    ]

    # Get normalized checkpoint base path
    checkpoint_base = get_checkpoint_full_path(checkpoint_base_path)

    checkpoint_dirs = [
        f"{checkpoint_base}/01_parse_documents",
        f"{checkpoint_base}/02_extract_page_content",
        f"{checkpoint_base}/03_classify_pages",
        f"{checkpoint_base}/04_extract_info_from_classified_pages",
    ]

    print("Dropping pipeline tables...")
    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")
            print(f"  Dropped table: {table}")
        except Exception as e:
            print(f"  Failed to drop table {table}: {str(e)}")

    print("\nCleaning checkpoint directories...")
    for checkpoint_dir in checkpoint_dirs:
        full_path = normalize_path(checkpoint_dir)
        try:
            if os.path.exists(full_path):
                shutil.rmtree(full_path)
                print(f"  Removed checkpoint: {full_path}")
            else:
                print(f"  Checkpoint directory not found: {full_path}")
        except Exception as e:
            print(f"  Failed to remove checkpoint {full_path}: {str(e)}")

    output_paths = [normalize_path(output_volume_path)]

    print("\nCleaning output directories...")
    for output_path in output_paths:
        try:
            if os.path.exists(output_path):
                for item in os.listdir(output_path):
                    item_path = os.path.join(output_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                print(f"  Cleaned output directory: {output_path}")
            else:
                print(f"  Output directory not found: {output_path}")
        except Exception as e:
            print(f"  Failed to clean output directory {output_path}: {str(e)}")

    print("\nPipeline cleanup completed!")


# COMMAND ----------

# Ensure directories exist
ensure_directories_exist()

# Clean pipeline if requested
if clean_pipeline == "Yes":
    print("CLEANING PIPELINE - This will remove all tables and checkpoints!")
    cleanup_pipeline()
else:
    print(
        "Cleanup skipped. Set 'Clean all pipeline tables and checkpoints?' to 'Yes' to proceed."
    )
    print("\nTables that would be cleaned:")
    print(f"  1. {raw_table_name} (parsed documents)")
    print(f"  2. {page_content_table_name} (page content)")
    print(f"  3. {classified_pages_table_name} (classified pages)")
    print(f"  4. {extracted_info_table_name} (extracted info)")
    checkpoint_base = get_checkpoint_full_path(checkpoint_base_path)
    print("\nCheckpoint directories that would be cleaned:")
    print(f"  - {checkpoint_base}/01_parse_documents")
    print(f"  - {checkpoint_base}/02_extract_page_content")
    print(f"  - {checkpoint_base}/03_classify_pages")
    print(f"  - {checkpoint_base}/04_extract_info_from_classified_pages")
    print("\nOutput directories that would be cleaned:")
    print(f"  - {normalize_path(output_volume_path)}")
