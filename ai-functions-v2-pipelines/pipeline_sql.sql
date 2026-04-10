-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PDF Document Processing Pipeline with AI Functions v2
-- MAGIC
-- MAGIC This notebook demonstrates an end-to-end document intelligence pipeline built entirely in SQL
-- MAGIC using three AI Functions introduced in Databricks:
-- MAGIC
-- MAGIC | Function | Purpose | Version |
-- MAGIC |----------|---------|---------|
-- MAGIC | `ai_parse_document` | OCR + layout-aware parsing of PDFs into structured VARIANT | v2.0 |
-- MAGIC | `ai_classify` | Zero-shot document classification with descriptive labels | v2 |
-- MAGIC | `ai_extract` | Structured field extraction with typed JSON schemas | v2 |
-- MAGIC
-- MAGIC **Architecture:** The pipeline follows the medallion pattern — bronze (parse), silver (classify),
-- MAGIC gold (type-specific extraction) — processing PDFs across 4 document types.
-- MAGIC
-- MAGIC **Data Source:** SharePoint via Lakeflow Connect standard connector (`databricks.connection`).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer — Document Parsing
-- MAGIC
-- MAGIC The bronze layer is the most expensive step: `ai_parse_document` runs OCR and a vision model
-- MAGIC on every PDF page to produce a structured VARIANT with text, tables, and figures.
-- MAGIC
-- MAGIC **Key design choice:** We use a CTE to call `ai_parse_document` exactly once per document.
-- MAGIC The parsed VARIANT is then reused to derive both the raw `parsed` column (passed downstream
-- MAGIC to silver/gold) and the `full_text` column (human-readable concatenation for debugging).
-- MAGIC
-- MAGIC The v2.0 output schema nests all content under `parsed:document:elements[]`, where each
-- MAGIC element has a `type` (text, table, figure) and `content` field.
-- MAGIC
-- MAGIC **Data Source:** Documents are ingested from SharePoint via Lakeflow Connect standard connector
-- MAGIC using `read_files` with `databricks.connection`.

-- COMMAND ----------

-- Stage 1: Parse all PDF documents from SharePoint (CTE to call ai_parse_document once)
CREATE OR REPLACE TABLE fins_genai.ai_function.batch_nb_bronze_parsed_docs AS
WITH raw_parsed AS (
    SELECT
        path,
        ai_parse_document(content, map('version', '2.0')) AS parsed
    FROM read_files(
        'https://databricks977.sharepoint.com/sites/brickfood-demo-site/archikademo/Forms/AllItems.aspx?npsAction=createList',
        `databricks.connection` => "sharepoint-lfc-e2-demo",
        format => 'binaryFile',
        pathGlobFilter => '*.pdf',
        schemaEvolutionMode => 'none'
    )
)
SELECT
    regexp_extract(path, '[^/]+$', 0) AS file_name,
    path,
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
-- MAGIC The silver layer applies `ai_classify` v2 to route each document into one of four categories.
-- MAGIC
-- MAGIC **What's new in v2:**
-- MAGIC - **VARIANT input** — we pass the raw `parsed` VARIANT directly instead of extracting text first.
-- MAGIC   The model sees the full structured document representation, improving classification accuracy.
-- MAGIC - **Descriptive labels** — instead of bare label names (`ARRAY('invoice', 'contract')`), v2 accepts
-- MAGIC   a JSON object where each key is a label and each value describes what that label means.
-- MAGIC   This gives the model richer context for ambiguous documents (e.g., a promissory note filed with
-- MAGIC   the SEC is a "sec_filing", not a "contract").
-- MAGIC - **VARIANT output** — returns `{"response": ["label"], "error": null}` instead of a plain STRING,
-- MAGIC   enabling error handling and multi-label support.

-- COMMAND ----------

-- Stage 2: Classify each document by type
CREATE OR REPLACE TABLE fins_genai.ai_function.batch_nb_silver_classified_docs AS
WITH classified AS (
    SELECT
        file_name,
        path,
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
    FROM fins_genai.ai_function.batch_nb_bronze_parsed_docs
)
SELECT
    file_name,
    path,
    parsed,
    full_text,
    classification:response[0]::STRING AS doc_type,
    classification:error::STRING AS classify_error
FROM classified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer — Type-Specific Field Extraction
-- MAGIC
-- MAGIC The gold layer is where the real power of `ai_extract` v2 shines. Each document type gets
-- MAGIC its own table with a purpose-built JSON schema that defines exactly what to extract.
-- MAGIC
-- MAGIC **What's new in v2:**
-- MAGIC - **Typed JSON schemas** — fields specify `"type": "string"`, `"number"`, `"array"`, `"object"`,
-- MAGIC   or `"enum"`, giving the model strict output constraints instead of free-form extraction.
-- MAGIC - **Nested structures** — invoices extract a `line_items` array of objects, each with description,
-- MAGIC   quantity, unit_price, and net_worth. Previously this required `ai_query` with a custom prompt.
-- MAGIC - **Enums** — `currency` and `contract_type` use `"type": "enum"` with allowed values, ensuring
-- MAGIC   consistent categorical output.
-- MAGIC - **Field descriptions** — each field can include a `"description"` hint (e.g.,
-- MAGIC   `"Issue date in YYYY-MM-DD format"`) to guide formatting without a separate `instructions` option.
-- MAGIC
-- MAGIC The routing pattern is simple: filter `batch_nb_silver_classified_docs` by `doc_type`, then apply the
-- MAGIC matching schema. Each gold table only calls `ai_extract` on its relevant subset of documents.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold: Invoices
-- MAGIC Extracts structured invoice data including nested `line_items` array — a capability
-- MAGIC that previously required `ai_query` with a custom prompt and `responseFormat`.

-- COMMAND ----------

-- Stage 3: Extract invoice fields
CREATE OR REPLACE TABLE fins_genai.ai_function.batch_nb_gold_invoices AS
WITH extracted AS (
    SELECT
        file_name,
        path,
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
    FROM fins_genai.ai_function.batch_nb_silver_classified_docs
    WHERE doc_type = 'invoice'
)
SELECT
    file_name,
    path,
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
-- MAGIC Extracts account summary fields. Flat schema — no nested objects needed.

-- COMMAND ----------

-- Stage 4: Extract bank statement fields
CREATE OR REPLACE TABLE fins_genai.ai_function.batch_nb_gold_bank_statements AS
WITH extracted AS (
    SELECT
        file_name,
        path,
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
    FROM fins_genai.ai_function.batch_nb_silver_classified_docs
    WHERE doc_type = 'bank_statement'
)
SELECT
    file_name,
    path,
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
-- MAGIC Uses `enum` type for `contract_type` to constrain output to known agreement categories,
-- MAGIC and an `array` of strings for `parties` to capture all contracting entities.

-- COMMAND ----------

-- Stage 5: Extract contract fields
CREATE OR REPLACE TABLE fins_genai.ai_function.batch_nb_gold_contracts AS
WITH extracted AS (
    SELECT
        file_name,
        path,
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
    FROM fins_genai.ai_function.batch_nb_silver_classified_docs
    WHERE doc_type = 'contract'
)
SELECT
    file_name,
    path,
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
-- MAGIC Extracts financial instrument details from SEC exhibits. The `principal_amount` field
-- MAGIC uses `"type": "number"` so it arrives as a numeric value ready for aggregation.

-- COMMAND ----------

-- Stage 6: Extract SEC filing fields
CREATE OR REPLACE TABLE fins_genai.ai_function.batch_nb_gold_sec_filings AS
WITH extracted AS (
    SELECT
        file_name,
        path,
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
    FROM fins_genai.ai_function.batch_nb_silver_classified_docs
    WHERE doc_type = 'sec_filing'
)
SELECT
    file_name,
    path,
    doc_type,
    extracted:response:document_type::STRING AS document_type,
    extracted:response:registrant::STRING AS registrant,
    extracted:response:filing_date::STRING AS filing_date,
    extracted:response:principal_amount::DOUBLE AS principal_amount,
    extracted:response:interest_rate::STRING AS interest_rate,
    extracted:response:maturity_date::STRING AS maturity_date,
    extracted:response:governing_law::STRING AS governing_law
FROM extracted;
