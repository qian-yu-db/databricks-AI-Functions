---
name: add-dab-workflow
description: Create a new Databricks Asset Bundle workflow in dab-workflows/ following this repo's patterns — Structured Streaming, ai_parse_document, run_workflow.sh, config.yaml
---

# Add DAB Workflow

Create a new DAB workflow under `dab-workflows/` that follows the established patterns in this repo.

## Directory structure

Every workflow follows this layout:

```
dab-workflows/<workflow-name>/
├── databricks.yml                  # Bundle config with variables
├── run_workflow.sh                 # CLI runner script
├── README.md                      # Workflow documentation
├── resources/
│   └── <workflow_name>.job.yml     # Job definition with tasks + dependencies
└── src/
    └── transformations/
        ├── config.yaml             # LLM model names and prompts
        ├── 00-clean-pipeline-tables.py   # Optional cleanup task
        ├── 01_<first_stage>.py
        ├── 02_<second_stage>.py
        └── ...
```

## Key conventions

### databricks.yml
- Always define variables for: `catalog`, `schema`, `source_volume_path`, `output_volume_path`, `checkpoint_base_path`, `clean_pipeline_tables`, and all table names
- Include `dev` (default, mode: development) and `prod` targets
- Use `include: [resources/*.yml]`

### Job definition (resources/*.job.yml)
- Use `job_clusters` with serverless or `i3.2xlarge` nodes
- Define `task_key` for each notebook task
- Wire `depends_on` for sequential/parallel task dependencies
- Pass all variables via `base_parameters` using `${var.<name>}` syntax
- Each task's `checkpoint_location` should be `${var.checkpoint_base_path}/<task_name>`

### Notebook tasks (src/transformations/*.py)
- Use Databricks notebook format with `# Databricks notebook source` header and `# COMMAND ----------` separators
- Read parameters via `dbutils.widgets.get("<param>")`
- Use Structured Streaming with `trigger(availableNow=True)` and `.awaitTermination()`
- Store checkpoint locations per task
- The cleanup task (00-clean-pipeline-tables.py) conditionally drops tables and removes checkpoints based on `clean_pipeline_tables` variable

### config.yaml
- Store LLM model names and prompt templates here, not in databricks.yml
- Notebooks read this file at runtime to keep prompts separate from infrastructure config

### run_workflow.sh
- Accepts `--profile`, `--target`, `--skip-validation`, `--skip-deployment` flags
- Validates, deploys, and runs the bundle via `databricks` CLI
- Optionally handles file upload/download for volumes

## After creating the workflow

1. Add the new workflow to `dab-workflows/README.md` under the Workflows section
2. Add a link in the top-level `README.md` under DAB Workflows
3. Validate with `databricks bundle validate --profile <profile>` before committing
