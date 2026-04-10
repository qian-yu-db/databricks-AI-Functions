# Databricks AI Functions

A collection of examples demonstrating how to use Databricks AI Functions for LLM batch inference and document processing. Each example is self-contained, making it simple for Data Analysts and Data Engineers to adapt for their own use cases.

## What are Databricks AI Functions?

Databricks AI Functions are SQL-native functions that bring LLM capabilities directly into SQL queries and Spark pipelines:

| Function | Purpose |
|----------|---------|
| [`ai_query`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query) | General-purpose LLM inference with custom prompts |
| [`ai_classify`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify) | Zero-shot text and document classification |
| [`ai_extract`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract) | Structured field extraction with typed JSON schemas |
| [`ai_parse_document`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document) | OCR + layout-aware document parsing |
| [`ai_summarize`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_summarize) | Text summarization |
| [`ai_analyze_sentiment`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_analyze_sentiment) | Sentiment analysis |

## Examples

### [AI Functions v1 Examples](./ai-functions-v1-examples)

Use-case walkthroughs with `ai_query`, `ai_classify`, `ai_summarize`, and `ai_analyze_sentiment`:

- **Insurance Call Center Analysis** — sentiment, compliance scoring, intent extraction, next-best-action
- **ML Feature Engineering** — generate categorical features from text using `ai_classify` and `ai_query`

### [AI Functions v1 Evaluation](./ai-functions-v1-evaluation)

MLflow 3 evaluation notebook for assessing AI Function output quality.

### [AI Functions v2 Pipelines](./ai-functions-v2-pipelines)

Document intelligence pipelines using AI Functions v2 (`ai_parse_document`, `ai_classify`, `ai_extract`) with Spark Declarative Pipelines. Implements medallion architecture (bronze/silver/gold) for processing PDFs across 4 document types. Includes batch and streaming variants in both Python and SQL.

### [DAB Workflows](./dab-workflows)

Production-ready Databricks Asset Bundle workflows for unstructured document processing with Structured Streaming:

- **[Unstructured IE](./dab-workflows/unstructured-ie)** — Parse, extract, analyze, and export structured entities as JSONL
- **[Parse-Translate-Classify](./dab-workflows/parse-translate-classify)** — Multi-lingual document segmentation and classification
- **[Knowledge Base](./dab-workflows/knowledge-base)** — 9-stage pipeline with diagram extraction, chunking, and visual enrichment for RAG
- **[IE Selected Pages](./dab-workflows/ie-selected-pages)** — Page-level classification for selective extraction from large documents

## Resources

- [Databricks AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions.html)
- [LLM Batch Inference](https://docs.databricks.com/aws/en/large-language-models/ai-query-batch-inference.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/declarative-pipelines/)
