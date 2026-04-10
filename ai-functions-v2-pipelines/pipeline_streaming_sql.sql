-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PDF Document Processing Pipeline with AI Functions v2 (Streaming)
-- MAGIC
-- MAGIC This notebook demonstrates an end-to-end **streaming** document intelligence pipeline built in SQL
-- MAGIC using three AI Functions and Lakeflow Connect for SharePoint ingestion:
-- MAGIC
-- MAGIC | Function | Purpose | Version |
-- MAGIC |----------|---------|---------|
-- MAGIC | `ai_parse_document` | OCR + layout-aware parsing of PDFs into structured VARIANT | v2.0 |
-- MAGIC | `ai_classify` | Zero-shot document classification with descriptive labels | v2 |
-- MAGIC | `ai_extract` | Structured field extraction with typed JSON schemas | v2 |
-- MAGIC
-- MAGIC **Architecture:** The pipeline follows the medallion pattern — bronze (ingest + parse), silver (classify),
-- MAGIC gold (type-specific extraction). **All stages are streaming tables**, making the entire pipeline
-- MAGIC fully incremental and append-only. Only new documents flow through each stage on refresh.
-- MAGIC
-- MAGIC **Data Source:** SharePoint via Lakeflow Connect standard connector (`databricks.connection`).
-- MAGIC
-- MAGIC **Trade-off vs materialized views:** Streaming tables are lower cost per refresh (append-only),
-- MAGIC but require a full refresh to pick up AI function upgrades or reprocess existing documents.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer — Streaming Ingestion from SharePoint
-- MAGIC
-- MAGIC The first bronze table uses `STREAM read_files` with `databricks.connection` to incrementally
-- MAGIC ingest new PDF files from SharePoint via Auto Loader. Each new file is captured exactly once
-- MAGIC as a streaming table row with its binary content and metadata.

-- COMMAND ----------

-- Stage 1: Incrementally ingest PDF files from SharePoint
CREATE OR REFRESH STREAMING TABLE stream_nb_bronze_raw_docs
COMMENT 'Incrementally ingested PDF files from SharePoint via Auto Loader'
AS
SELECT
    path,
    content,
    length,
    current_timestamp() AS _ingested_at,
    _metadata.file_modification_time AS _file_modified_at
FROM STREAM read_files(
    'https://databricks977.sharepoint.com/sites/brickfood-demo-site/archikademo/Forms/AllItems.aspx?npsAction=createList',
    `databricks.connection` => "sharepoint-lfc-e2-demo",
    format => 'binaryFile',
    pathGlobFilter => '*.pdf',
    schemaEvolutionMode => 'none'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer — Document Parsing
-- MAGIC
-- MAGIC Reads new rows from `stream_nb_bronze_raw_docs` via `stream()` and applies `ai_parse_document`.
-- MAGIC As a streaming table, each document is parsed exactly once — new PDFs are parsed
-- MAGIC incrementally on each pipeline refresh.

-- COMMAND ----------

-- Stage 2: Parse newly ingested PDF documents
CREATE OR REFRESH STREAMING TABLE stream_nb_bronze_parsed_docs
COMMENT 'Parsed PDF documents with ai_parse_document v2.0'
AS
WITH raw_parsed AS (
    SELECT
        path,
        _ingested_at,
        ai_parse_document(content, map('version', '2.0')) AS parsed
    FROM stream(stream_nb_bronze_raw_docs)
)
SELECT
    regexp_extract(path, '[^/]+$', 0) AS file_name,
    path,
    _ingested_at,
    parsed,
    concat_ws(
        '\n\n',
        transform(
            try_cast(parsed:document:elements AS ARRAY<STRUCT<content:STRING>>),
            el -> el.content
        )
    ) AS full_text
FROM raw_parsed
WHERE cast(parsed:error_status AS STRING) IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer — Document Classification
-- MAGIC
-- MAGIC Reads new parsed documents from `stream_nb_bronze_parsed_docs` via `stream()` and classifies
-- MAGIC each into one of four categories. Append-only — each document is classified once.

-- COMMAND ----------

-- Stage 3: Classify each document by type
CREATE OR REFRESH STREAMING TABLE stream_nb_silver_classified_docs
COMMENT 'Documents classified by type using ai_classify v2'
AS
WITH classified AS (
    SELECT
        file_name,
        path,
        _ingested_at,
        parsed,
        full_text,
        ai_classify(
            parsed,
            '{
                "invoice": "Commercial invoice with seller, buyer, line items, and total amounts",
                "bank_statement": "Bank account statement with balances, deposits, and withdrawals",
                "contract": "Legal agreement or contract between parties with terms and obligations",
                "sec_filing": "Securities filing such as promissory notes or financial disclosures"
            }'
        ) AS classification
    FROM stream(stream_nb_bronze_parsed_docs)
)
SELECT
    file_name,
    path,
    _ingested_at,
    parsed,
    full_text,
    classification:response[0]::STRING AS doc_type,
    classification:error::STRING AS classify_error
FROM classified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer — Type-Specific Field Extraction
-- MAGIC
-- MAGIC Each document type gets its own streaming table. New classified documents flow through
-- MAGIC `stream(stream_nb_silver_classified_docs)`, are filtered by `doc_type`, and extracted once.
-- MAGIC Append-only — previously extracted documents are not reprocessed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold: Invoices

-- COMMAND ----------

-- Stage 4: Extract invoice fields
CREATE OR REFRESH STREAMING TABLE stream_nb_gold_invoices
COMMENT 'Extracted invoice details using ai_extract v2'
AS
WITH extracted AS (
    SELECT
        file_name,
        path,
        _ingested_at,
        doc_type,
        ai_extract(
            parsed,
            '{
                "invoice_number": {"type": "string", "description": "Invoice identifier"},
                "date_of_issue": {"type": "string", "description": "Issue date in YYYY-MM-DD format"},
                "seller_name": {"type": "string"},
                "seller_tax_id": {"type": "string"},
                "client_name": {"type": "string"},
                "client_tax_id": {"type": "string"},
                "line_items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "description": {"type": "string"},
                            "quantity": {"type": "number"},
                            "unit_price": {"type": "number"},
                            "net_worth": {"type": "number"}
                        }
                    }
                },
                "net_total": {"type": "number"},
                "vat_total": {"type": "number"},
                "gross_total": {"type": "number"},
                "currency": {"type": "enum", "labels": ["USD", "EUR", "GBP"], "description": "Currency code"}
            }'
        ) AS extracted
    FROM stream(stream_nb_silver_classified_docs)
    WHERE doc_type = 'invoice'
)
SELECT
    file_name,
    path,
    _ingested_at,
    doc_type,
    extracted:response:invoice_number::STRING AS invoice_number,
    extracted:response:date_of_issue::STRING AS date_of_issue,
    extracted:response:seller_name::STRING AS seller_name,
    extracted:response:seller_tax_id::STRING AS seller_tax_id,
    extracted:response:client_name::STRING AS client_name,
    extracted:response:client_tax_id::STRING AS client_tax_id,
    extracted:response:line_items AS line_items,
    extracted:response:net_total::DOUBLE AS net_total,
    extracted:response:vat_total::DOUBLE AS vat_total,
    extracted:response:gross_total::DOUBLE AS gross_total,
    extracted:response:currency::STRING AS currency
FROM extracted;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold: Bank Statements

-- COMMAND ----------

-- Stage 5: Extract bank statement fields
CREATE OR REFRESH STREAMING TABLE stream_nb_gold_bank_statements
COMMENT 'Extracted bank statement details using ai_extract v2'
AS
WITH extracted AS (
    SELECT
        file_name,
        path,
        _ingested_at,
        doc_type,
        ai_extract(
            parsed,
            '{
                "bank_name": {"type": "string"},
                "account_holder": {"type": "string"},
                "account_number": {"type": "string"},
                "account_type": {"type": "string"},
                "statement_date": {"type": "string", "description": "Statement date in YYYY-MM-DD format"},
                "beginning_balance": {"type": "number"},
                "ending_balance": {"type": "number"},
                "total_deposits": {"type": "number"},
                "total_withdrawals": {"type": "number"}
            }'
        ) AS extracted
    FROM stream(stream_nb_silver_classified_docs)
    WHERE doc_type = 'bank_statement'
)
SELECT
    file_name,
    path,
    _ingested_at,
    doc_type,
    extracted:response:bank_name::STRING AS bank_name,
    extracted:response:account_holder::STRING AS account_holder,
    extracted:response:account_number::STRING AS account_number,
    extracted:response:account_type::STRING AS account_type,
    extracted:response:statement_date::STRING AS statement_date,
    extracted:response:beginning_balance::DOUBLE AS beginning_balance,
    extracted:response:ending_balance::DOUBLE AS ending_balance,
    extracted:response:total_deposits::DOUBLE AS total_deposits,
    extracted:response:total_withdrawals::DOUBLE AS total_withdrawals
FROM extracted;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold: Contracts

-- COMMAND ----------

-- Stage 6: Extract contract fields
CREATE OR REFRESH STREAMING TABLE stream_nb_gold_contracts
COMMENT 'Extracted contract details using ai_extract v2'
AS
WITH extracted AS (
    SELECT
        file_name,
        path,
        _ingested_at,
        doc_type,
        ai_extract(
            parsed,
            '{
                "contract_title": {"type": "string"},
                "contract_type": {"type": "enum", "labels": ["employment agreement", "co-branding agreement", "security agreement", "agency agreement", "other"]},
                "parties": {"type": "array", "items": {"type": "string"}, "description": "Names of contracting parties"},
                "effective_date": {"type": "string", "description": "Effective date in YYYY-MM-DD format"},
                "term_length": {"type": "string", "description": "Duration of the agreement"},
                "governing_law": {"type": "string", "description": "State or jurisdiction"}
            }'
        ) AS extracted
    FROM stream(stream_nb_silver_classified_docs)
    WHERE doc_type = 'contract'
)
SELECT
    file_name,
    path,
    _ingested_at,
    doc_type,
    extracted:response:contract_title::STRING AS contract_title,
    extracted:response:contract_type::STRING AS contract_type,
    extracted:response:parties AS parties,
    extracted:response:effective_date::STRING AS effective_date,
    extracted:response:term_length::STRING AS term_length,
    extracted:response:governing_law::STRING AS governing_law
FROM extracted;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold: SEC Filings

-- COMMAND ----------

-- Stage 7: Extract SEC filing fields
CREATE OR REFRESH STREAMING TABLE stream_nb_gold_sec_filings
COMMENT 'Extracted SEC filing details using ai_extract v2'
AS
WITH extracted AS (
    SELECT
        file_name,
        path,
        _ingested_at,
        doc_type,
        ai_extract(
            parsed,
            '{
                "document_type": {"type": "string", "description": "e.g. Promissory Note, Form 10-K"},
                "registrant": {"type": "string", "description": "Company name"},
                "filing_date": {"type": "string", "description": "Filing date in YYYY-MM-DD format"},
                "principal_amount": {"type": "number"},
                "interest_rate": {"type": "string"},
                "maturity_date": {"type": "string", "description": "Maturity date in YYYY-MM-DD format"},
                "governing_law": {"type": "string"}
            }'
        ) AS extracted
    FROM stream(stream_nb_silver_classified_docs)
    WHERE doc_type = 'sec_filing'
)
SELECT
    file_name,
    path,
    _ingested_at,
    doc_type,
    extracted:response:document_type::STRING AS document_type,
    extracted:response:registrant::STRING AS registrant,
    extracted:response:filing_date::STRING AS filing_date,
    extracted:response:principal_amount::DOUBLE AS principal_amount,
    extracted:response:interest_rate::STRING AS interest_rate,
    extracted:response:maturity_date::STRING AS maturity_date,
    extracted:response:governing_law::STRING AS governing_law
FROM extracted;
