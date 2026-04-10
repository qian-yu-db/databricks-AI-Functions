import pandas as pd
from typing import Iterator

from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Required (per Databricks docs) for transformWithState-style custom stateful operators
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
)  # :contentReference[oaicite:2]{index=2}

# Output schema: one row per completed PDF
output_schema = StructType([
    StructField("pdf_id", StringType(), False),
    StructField("full_text", StringType(), True),
    StructField("page_count", IntegerType(), True),
])

class PdfAssembleProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # MapState: page_num -> page_text
        self.pages = handle.getMapState(
            stateName="pages",
            userKeySchema="page_num int",
            valueSchema="page_text string",
        )
        # ValueState: total_pages (store as int)
        self.total = handle.getValueState(
            stateName="total_pages",
            schema="total_pages int"
        )

    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timerValues) -> Iterator[pd.DataFrame]:
        pdf_id = key  # because we groupBy("pdf_id") below

        # rows is an iterator of pandas DataFrames for this key in this micro-batch
        for pdf in rows:
            # Update state from each row
            for r in pdf.itertuples(index=False):
                # r.pdf_id, r.page_num, r.total_pages, r.page_text
                self.pages.updateValue((int(r.page_num),), (str(r.page_text),))
                # keep the latest total_pages we see
                self.total.update((int(r.total_pages),))

        # Decide if complete
        total_tuple = self.total.get()
        if total_tuple is None:
            return
        total_pages = total_tuple[0]

        # How many pages do we have?
        seen_pages = list(self.pages.keys())  # iterator -> list of (page_num,) tuples
        if len(seen_pages) < total_pages:
            return  # not complete yet; emit nothing

        # Build full text in page order
        page_nums = sorted([k[0] for k in seen_pages])
        full_text = "\n".join([self.pages.getValue((p,))[0] for p in page_nums])

        # Clear state for this pdf_id so we don't re-emit forever
        self.pages.clear()
        self.total.clear()

        yield pd.DataFrame([{
            "pdf_id": pdf_id,
            "full_text": full_text,
            "page_count": len(page_nums),
        }])

# df must have columns: pdf_id, page_num, total_pages, page_text
# Example: df = spark.readStream.format("...").load(...)

assembled = (
    df.groupBy("pdf_id")
      .transformWithStateInPandas(
          statefulProcessor=PdfAssembleProcessor(),
          outputStructType=output_schema,
          outputMode="Append",   # only output when complete
          timeMode="None",       # keep it simple; you can use event/processing time modes too
      )
)

query = (
    assembled.writeStream
      .format("delta")
      .option("checkpointLocation", "/tmp/chk/pdf_assemble")
      .option("path", "/tmp/out/pdf_assemble")
      .start()
)