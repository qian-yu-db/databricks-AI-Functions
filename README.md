# Databricks AI Functions

A collection of examples demonstrating how to use Databricks AI Functions for LLM batch inference and document processing. Each example is self-contained, making it simple for Data Analysts and Data Engineers to adapt for their own use cases.

## What are Databricks AI Functions?

Databricks AI Functions are SQL-native functions that bring LLM capabilities directly into SQL queries and Spark pipelines — no model endpoint setup required. Always prefer a task-specific function over `ai_query`.

### Task-Specific Functions

| Function | Purpose | v2 Enhancements |
|----------|---------|-----------------|
| [`ai_classify`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify) | Zero-shot classification | Accepts VARIANT input (parsed docs) and descriptive label maps (`{"label": "description"}`) instead of plain arrays |
| [`ai_extract`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract) | Structured field extraction | Accepts VARIANT input and typed JSON schemas with `string`, `number`, `array`, `object`, `enum` types; supports nested structures and field descriptions |
| [`ai_parse_document`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document) | OCR + layout-aware document parsing | v2.0 output schema nests content under `document:elements[]` with bounding boxes; supports PDF, DOCX, PPTX, JPG, PNG |
| [`ai_analyze_sentiment`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_analyze_sentiment) | Sentiment analysis | |
| [`ai_summarize`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_summarize) | Text summarization | |
| [`ai_translate`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_translate) | Translation (8 languages) | |
| [`ai_mask`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_mask) | PII redaction | |
| [`ai_similarity`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_similarity) | Semantic similarity scoring | |
| [`ai_fix_grammar`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_fix_grammar) | Grammar correction | |
| [`ai_gen`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_gen) | Free-form text generation | |

### General-Purpose & Table-Valued

| Function | Purpose |
|----------|---------|
| [`ai_query`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query) | Custom prompts, nested JSON extraction, multimodal input, custom endpoints — use as **last resort** when no task-specific function fits |
| [`ai_forecast`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_forecast) | Time series forecasting (requires Pro or Serverless SQL warehouse) |

## Examples

### [AI Functions v1 Examples](./ai-functions-v1-examples)

Use-case walkthroughs using v1 function signatures (`ai_query` with string prompts, `ai_classify` with `ARRAY` labels, `ai_summarize`, `ai_analyze_sentiment`):

- **Insurance Call Center Analysis** — sentiment, compliance scoring, intent extraction, next-best-action
- **ML Feature Engineering** — generate categorical features from text using `ai_classify` and `ai_query`

### [AI Functions v1 Evaluation](./ai-functions-v1-evaluation)

MLflow 3 evaluation notebook for assessing AI Function output quality.

### [AI Functions v2 Pipelines](./ai-functions-v2-pipelines)

Document intelligence pipelines showcasing **v2 function signatures** with Spark Declarative Pipelines. Demonstrates:

- **`ai_parse_document` v2.0** — VARIANT output with `document:elements[]` structure and bounding boxes
- **`ai_classify` v2** — VARIANT input + descriptive label maps (`{"invoice": "Commercial invoice with..."}`) for higher-accuracy classification
- **`ai_extract` v2** — VARIANT input + typed JSON schemas with nested arrays, enums, and field descriptions

Implements medallion architecture (bronze/silver/gold) for processing PDFs across 4 document types (invoices, bank statements, contracts, SEC filings). Includes batch and streaming variants in both Python and SQL.

### [DAB Workflows](./dab-workflows)

Production-ready Databricks Asset Bundle workflows for unstructured document processing with Structured Streaming:

- **[Unstructured IE](./dab-workflows/unstructured-ie)** — Parse, extract, analyze, and export structured entities as JSONL
- **[Parse-Translate-Classify](./dab-workflows/parse-translate-classify)** — Multi-lingual document segmentation and classification
- **[Knowledge Base](./dab-workflows/knowledge-base)** — 9-stage pipeline with diagram extraction, chunking, and visual enrichment for RAG
- **[IE Selected Pages](./dab-workflows/ie-selected-pages)** — Page-level classification for selective extraction from large documents

## TODO: Migrate DAB Workflows to v2 Function Signatures

The DAB workflows currently use `ai_parse_document` v2.0 but rely on `ai_query` for classification and extraction tasks that v2 task-specific functions can now handle directly.

### High Priority

- [ ] **`ie-selected-pages/03_classify_pages.py`** — Replace `ai_query` (Yes/No string matching) with `ai_classify` v2. Eliminates custom prompt, string parsing, and label matching logic.
- [ ] **`ie-selected-pages/04_extract_info.py`** — Replace `ai_query` with `ai_extract` v2. Flat schema (`pws_id`, `sample_category`) is a textbook `ai_extract` use case.

### Medium Priority

- [ ] **`unstructured-ie/03_extract_key_info.py`** — Replace `ai_query` with `ai_extract` v2 for bond data extraction. Nested schemas now supported in v2. Keep agent_bricks path as fallback.
- [ ] **`knowledge-base/04_2_extract_key_info.py`** — Same pattern as above for electronics datasheet extraction.

### Low Priority / Trade-offs

- [ ] **`parse-translate-classify/02_translate_content.py`** — Consider `ai_translate(content, 'en')` instead of `ai_query`. Simpler but loses the custom "retain original formatting" prompt instruction.

### No Change Needed

- **`knowledge-base/04_1` (diagram enrichment)** — Requires multimodal image input; no task-specific function available.
- **`parse-translate-classify/03` (segmentation)** — Complex multi-step reasoning (segment + classify + transform in one pass); `ai_query` is the right tool.

## Resources

- [Databricks AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions.html)
- [LLM Batch Inference](https://docs.databricks.com/aws/en/large-language-models/ai-query-batch-inference.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/declarative-pipelines/)
