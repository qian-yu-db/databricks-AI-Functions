# AI Functions v2 Pipelines

End-to-end document intelligence pipelines using Databricks AI Functions v2 (`ai_parse_document`, `ai_classify`, `ai_extract`) with Spark Declarative Pipelines (SDP).

## Overview

These pipelines demonstrate a **medallion architecture** for processing PDFs across 4 document types (invoices, bank statements, contracts, SEC filings):

- **Bronze**: Parse PDFs with `ai_parse_document v2.0` (OCR + layout-aware extraction)
- **Silver**: Classify documents with `ai_classify v2` (zero-shot with descriptive labels)
- **Gold**: Extract type-specific fields with `ai_extract v2` (typed JSON schemas)

**Data Source**: SharePoint via Lakeflow Connect standard connector.

## Pipeline Variants

| File | Pattern | Approach |
|------|---------|----------|
| `pipeline.py` | Batch SDP (Python) | Materialized views — full recompute on refresh |
| `pipeline_streaming.py` | Streaming SDP (Python) | Streaming tables with Auto Loader — incremental, append-only |
| `pipeline_sql.sql` | Batch notebook (SQL) | `CREATE OR REPLACE TABLE` — imperative SQL, full recompute |
| `pipeline_streaming_sql.sql` | Streaming notebook (SQL) | `CREATE OR REFRESH STREAMING TABLE` — incremental SQL |

### When to use which

- **Batch (materialized views)**: Simpler, picks up AI function upgrades automatically on refresh. Higher cost per refresh.
- **Streaming (tables)**: Lower cost per refresh (append-only), but requires full refresh to reprocess existing documents or pick up AI function changes.
- **Python SDP**: Best for programmatic schema definitions and reusable constants.
- **SQL notebooks**: Best for interactive exploration and teams that prefer SQL.

## AI Functions Used

| Function | Version | Purpose |
|----------|---------|---------|
| `ai_parse_document` | v2.0 | OCR + layout-aware parsing of PDFs into structured VARIANT |
| `ai_classify` | v2 | Zero-shot classification with descriptive label maps |
| `ai_extract` | v2 | Typed JSON schema extraction (string, number, array, enum) |

### What's new in v2

- **VARIANT input/output** — pass raw parsed VARIANT directly to `ai_classify` and `ai_extract`
- **Descriptive labels** — `ai_classify` accepts JSON objects with label descriptions
- **Typed schemas** — `ai_extract` supports `string`, `number`, `array`, `object`, `enum` types
- **Nested structures** — extract arrays of objects (e.g., invoice line items)
- **Field descriptions** — guide formatting without separate instructions

## Document Types & Extraction Schemas

- **Invoices**: invoice number, dates, seller/client info, line items (nested array), totals, currency (enum)
- **Bank Statements**: bank name, account holder, account number, balances, deposits/withdrawals
- **Contracts**: title, type (enum), parties (array), effective date, term, governing law
- **SEC Filings**: document type, registrant, filing date, principal amount, interest rate, maturity date

## Test Documents

The pipelines were tested with 13 sample PDFs across the 4 document types:

| Type | Files | Description |
|------|-------|-------------|
| Invoice | `electronic_invoices_*.pdf` (4) | Commercial invoices with seller/buyer info, line items, and totals |
| Bank Statement | `bank_statement_sample{1,2,3}.pdf` (3) | Account statements with balances, deposits, and withdrawals |
| Contract | 3 files (Co-Branding, Agency, Alliance agreements) | Legal agreements between parties with terms and obligations |
| SEC Filing | `sec_000{2,3,9}.pdf` (3) | Securities filings such as promissory notes and financial disclosures |

PDFs are not included in the repo. To test, place your own documents in a Unity Catalog Volume and update the `SHAREPOINT_URL` / `SHAREPOINT_CONNECTION` constants in the pipeline files, or replace the data source with `spark.read.format("binaryFile").load("/Volumes/...")`.

## Resources

- [`ai_parse_document`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document)
- [`ai_classify`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify)
- [`ai_extract`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract)
- [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/declarative-pipelines/)
- [Lakeflow Connect](https://docs.databricks.com/aws/en/connect/)
