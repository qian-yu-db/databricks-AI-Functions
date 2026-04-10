import json

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr

SHAREPOINT_URL = "https://databricks977.sharepoint.com/sites/brickfood-demo-site/archikademo/Forms/AllItems.aspx?npsAction=createList"
SHAREPOINT_CONNECTION = "sharepoint-lfc-e2-demo"

# ── ai_classify v2: labels with descriptions ────────────────────────────────
CLASSIFY_LABELS = json.dumps({
    "invoice": "Commercial invoice with seller, buyer, line items, and total amounts",
    "bank_statement": "Bank account statement with balances, deposits, and withdrawals",
    "contract": "Legal agreement or contract between parties with terms and obligations",
    "sec_filing": "Securities filing such as promissory notes or financial disclosures",
})

# ── ai_extract v2: type-specific JSON schemas ──────────────────────────────
INVOICE_SCHEMA = json.dumps({
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
                "net_worth": {"type": "number"},
            },
        },
    },
    "net_total": {"type": "number"},
    "vat_total": {"type": "number"},
    "gross_total": {"type": "number"},
    "currency": {
        "type": "enum",
        "labels": ["USD", "EUR", "GBP"],
        "description": "Currency code",
    },
})

BANK_STATEMENT_SCHEMA = json.dumps({
    "bank_name": {"type": "string"},
    "account_holder": {"type": "string"},
    "account_number": {"type": "string"},
    "account_type": {"type": "string"},
    "statement_date": {"type": "string", "description": "Statement date in YYYY-MM-DD format"},
    "beginning_balance": {"type": "number"},
    "ending_balance": {"type": "number"},
    "total_deposits": {"type": "number"},
    "total_withdrawals": {"type": "number"},
})

CONTRACT_SCHEMA = json.dumps({
    "contract_title": {"type": "string"},
    "contract_type": {
        "type": "enum",
        "labels": [
            "employment agreement",
            "co-branding agreement",
            "security agreement",
            "agency agreement",
            "other",
        ],
    },
    "parties": {
        "type": "array",
        "items": {"type": "string"},
        "description": "Names of contracting parties",
    },
    "effective_date": {"type": "string", "description": "Effective date in YYYY-MM-DD format"},
    "term_length": {"type": "string", "description": "Duration of the agreement"},
    "governing_law": {"type": "string", "description": "State or jurisdiction"},
})

SEC_FILING_SCHEMA = json.dumps({
    "document_type": {"type": "string", "description": "e.g. Promissory Note, Form 10-K"},
    "registrant": {"type": "string", "description": "Company name"},
    "filing_date": {"type": "string", "description": "Filing date in YYYY-MM-DD format"},
    "principal_amount": {"type": "number"},
    "interest_rate": {"type": "string"},
    "maturity_date": {"type": "string", "description": "Maturity date in YYYY-MM-DD format"},
    "governing_law": {"type": "string"},
})


# ── Stage 1: Ingest PDFs from SharePoint via Auto Loader (streaming) ────────
@dp.table(
    name="stream_sdp_bronze_raw_docs",
    comment="Incrementally ingested PDF files from SharePoint via Auto Loader",
)
def stream_sdp_bronze_raw_docs():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("databricks.connection", SHAREPOINT_CONNECTION)
        .option("pathGlobFilter", "*.pdf")
        .load(SHAREPOINT_URL)
        .select(
            col("path"),
            col("content"),
            col("length"),
            current_timestamp().alias("_ingested_at"),
            col("_metadata.file_modification_time").alias("_file_modified_at"),
        )
    )


# ── Stage 2: Parse PDF documents ────────────────────────────────────────────
@dp.materialized_view(
    name="stream_sdp_bronze_parsed_docs",
    comment="Parsed PDF documents with ai_parse_document v2.0",
)
def stream_sdp_bronze_parsed_docs():
    return (
        spark.read.table("stream_sdp_bronze_raw_docs")
        .withColumn("parsed", expr("ai_parse_document(content, map('version', '2.0'))"))
        .selectExpr(
            "regexp_extract(path, '[^/]+$', 0) AS file_name",
            "path",
            "_ingested_at",
            "parsed",
            "concat_ws('\\n\\n', transform(try_cast(parsed:document:elements AS ARRAY<STRUCT<content:STRING>>), el -> el.content)) AS full_text",
            "cast(parsed:error_status AS STRING) AS parse_error",
        )
        .filter("parse_error IS NULL")
    )


# ── Stage 3: Classify documents by type ─────────────────────────────────────
@dp.materialized_view(
    name="stream_sdp_silver_classified_docs",
    comment="Documents classified by type using ai_classify v2",
)
def stream_sdp_silver_classified_docs():
    return (
        spark.read.table("stream_sdp_bronze_parsed_docs")
        .withColumn("classification", expr(f"ai_classify(parsed, '{CLASSIFY_LABELS}')"))
        .selectExpr(
            "file_name",
            "path",
            "_ingested_at",
            "parsed",
            "full_text",
            "classification:response[0]::STRING AS doc_type",
            "classification:error::STRING AS classify_error",
        )
    )


# ── Stage 4: Extract invoice fields ─────────────────────────────────────────
@dp.materialized_view(
    name="stream_sdp_gold_invoices",
    comment="Extracted invoice details using ai_extract v2",
)
def stream_sdp_gold_invoices():
    return (
        spark.read.table("stream_sdp_silver_classified_docs")
        .filter("doc_type = 'invoice'")
        .withColumn("extracted", expr(f"ai_extract(parsed, '{INVOICE_SCHEMA}')"))
        .selectExpr(
            "file_name",
            "path",
            "_ingested_at",
            "doc_type",
            "extracted:response:invoice_number::STRING AS invoice_number",
            "extracted:response:date_of_issue::STRING AS date_of_issue",
            "extracted:response:seller_name::STRING AS seller_name",
            "extracted:response:seller_tax_id::STRING AS seller_tax_id",
            "extracted:response:client_name::STRING AS client_name",
            "extracted:response:client_tax_id::STRING AS client_tax_id",
            "extracted:response:line_items AS line_items",
            "extracted:response:net_total::DOUBLE AS net_total",
            "extracted:response:vat_total::DOUBLE AS vat_total",
            "extracted:response:gross_total::DOUBLE AS gross_total",
            "extracted:response:currency::STRING AS currency",
        )
    )


# ── Stage 5: Extract bank statement fields ──────────────────────────────────
@dp.materialized_view(
    name="stream_sdp_gold_bank_statements",
    comment="Extracted bank statement details using ai_extract v2",
)
def stream_sdp_gold_bank_statements():
    return (
        spark.read.table("stream_sdp_silver_classified_docs")
        .filter("doc_type = 'bank_statement'")
        .withColumn("extracted", expr(f"ai_extract(parsed, '{BANK_STATEMENT_SCHEMA}')"))
        .selectExpr(
            "file_name",
            "path",
            "_ingested_at",
            "doc_type",
            "extracted:response:bank_name::STRING AS bank_name",
            "extracted:response:account_holder::STRING AS account_holder",
            "extracted:response:account_number::STRING AS account_number",
            "extracted:response:account_type::STRING AS account_type",
            "extracted:response:statement_date::STRING AS statement_date",
            "extracted:response:beginning_balance::DOUBLE AS beginning_balance",
            "extracted:response:ending_balance::DOUBLE AS ending_balance",
            "extracted:response:total_deposits::DOUBLE AS total_deposits",
            "extracted:response:total_withdrawals::DOUBLE AS total_withdrawals",
        )
    )


# ── Stage 6: Extract contract fields ────────────────────────────────────────
@dp.materialized_view(
    name="stream_sdp_gold_contracts",
    comment="Extracted contract details using ai_extract v2",
)
def stream_sdp_gold_contracts():
    return (
        spark.read.table("stream_sdp_silver_classified_docs")
        .filter("doc_type = 'contract'")
        .withColumn("extracted", expr(f"ai_extract(parsed, '{CONTRACT_SCHEMA}')"))
        .selectExpr(
            "file_name",
            "path",
            "_ingested_at",
            "doc_type",
            "extracted:response:contract_title::STRING AS contract_title",
            "extracted:response:contract_type::STRING AS contract_type",
            "extracted:response:parties AS parties",
            "extracted:response:effective_date::STRING AS effective_date",
            "extracted:response:term_length::STRING AS term_length",
            "extracted:response:governing_law::STRING AS governing_law",
        )
    )


# ── Stage 7: Extract SEC filing fields ──────────────────────────────────────
@dp.materialized_view(
    name="stream_sdp_gold_sec_filings",
    comment="Extracted SEC filing details using ai_extract v2",
)
def stream_sdp_gold_sec_filings():
    return (
        spark.read.table("stream_sdp_silver_classified_docs")
        .filter("doc_type = 'sec_filing'")
        .withColumn("extracted", expr(f"ai_extract(parsed, '{SEC_FILING_SCHEMA}')"))
        .selectExpr(
            "file_name",
            "path",
            "_ingested_at",
            "doc_type",
            "extracted:response:document_type::STRING AS document_type",
            "extracted:response:registrant::STRING AS registrant",
            "extracted:response:filing_date::STRING AS filing_date",
            "extracted:response:principal_amount::DOUBLE AS principal_amount",
            "extracted:response:interest_rate::STRING AS interest_rate",
            "extracted:response:maturity_date::STRING AS maturity_date",
            "extracted:response:governing_law::STRING AS governing_law",
        )
    )
