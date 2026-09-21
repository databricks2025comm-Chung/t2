# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/landing_writer
# MAGIC Buffers paginated API records in fixed-size chunks and writes each chunk
# MAGIC as a Parquet append to the landing zone.
# MAGIC
# MAGIC Memory bound: at most `_CHUNK_RECORDS` records are held in driver memory
# MAGIC at any time, so large historical loads do not OOM the driver node.
# MAGIC
# MAGIC Trade-off: a run that spans multiple chunks produces multiple Parquet files
# MAGIC at the landing path.  For incremental (watermark-filtered) runs the chunk
# MAGIC threshold is rarely reached, so most runs still produce a single file.
# MAGIC For large historical loads, compact the landing path downstream with a
# MAGIC write-to-tmp-then-swap pattern (reading and overwriting the same path in one
# MAGIC Spark action can corrupt files that are still being read):
# MAGIC
# MAGIC   tmp = path.rstrip("/") + "_compact_tmp/"
# MAGIC   spark.read.parquet(path).repartition(n).write.mode("overwrite").parquet(tmp)
# MAGIC   dbutils.fs.rm(path, recurse=True)
# MAGIC   dbutils.fs.mv(tmp, path)
# MAGIC
# MAGIC Depends on: `logging_utils` (must be `%run` first).
# MAGIC
# MAGIC **Schema drift across chunks**
# MAGIC `spark.createDataFrame(records)` infers schema independently per chunk, so
# MAGIC two chunks whose records have different key sets produce Parquet files with
# MAGIC different schemas at the same path.  This is intentional — enforcing the
# MAGIC schema from chunk 1 at write time would silently drop fields that first
# MAGIC appear in later chunks.  Instead, any reader of the landing path must use
# MAGIC `spark.read.option("mergeSchema", "true").parquet(path)`.  The entity
# MAGIC extractor already does this for the watermark read.  Downstream silver-layer
# MAGIC jobs should do the same, or set the session conf:
# MAGIC   spark.conf.set("spark.sql.parquet.mergeSchema", "true")

# COMMAND ----------

_CHUNK_RECORDS = 50_000   # flush buffer to Parquet after this many records (~50 MB at 1 KB/record)

# COMMAND ----------

def _write_chunk(
    spark,
    records: list,
    path:    str,
    log:     "ContextLogger",
) -> None:
    """Append one buffer of records as a Parquet file."""
    spark.createDataFrame(records).write.mode("append").parquet(path)


def write_pages_to_parquet(
    page_iter:    "Iterator[list[dict]]",
    landing_path: str,
    spark,
    log:          "ContextLogger",
) -> int:
    """
    Consume the page generator, write records to Parquet in fixed-size chunks.

    Each chunk is an append to `landing_path`, so the directory accumulates one
    file per chunk.  For most incremental runs the chunk threshold is never hit
    and only a single file is written.

    Args:
        page_iter:    Generator from a PAGINATION_DISPATCH handler.
        landing_path: Parquet destination in the landing zone.
        spark:        Active SparkSession.
        log:          ContextLogger from logging_utils.

    Returns:
        Total number of records written.
    """
    buffer    = []
    total     = 0
    chunk_num = 0

    for page_num, batch in enumerate(page_iter):
        buffer.extend(batch)
        log.debug(
            f"Page buffered  page={page_num + 1}"
            f"  size={len(batch)}"
            f"  buffer={len(buffer)}"
        )

        if len(buffer) >= _CHUNK_RECORDS:
            _write_chunk(spark, buffer, landing_path, log)
            total     += len(buffer)
            chunk_num += 1
            log.debug(f"Chunk flushed  chunk={chunk_num}  running_total={total}")
            buffer = []

    # flush any remaining records that did not fill a full chunk
    if buffer:
        _write_chunk(spark, buffer, landing_path, log)
        total     += len(buffer)
        chunk_num += 1

    if total == 0:
        log.info("No records returned by API — landing zone not written")
        return 0

    log.info(
        f"Parquet write complete"
        f"  records={total}"
        f"  chunks={chunk_num}"
        f"  path={landing_path}"
    )
    return total
