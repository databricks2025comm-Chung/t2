# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/landing_writer
# MAGIC Streams `ApiPage` objects to a tmp JSONL directory page-by-page, then
# MAGIC consolidates to Parquet in one Spark read pass.
# MAGIC
# MAGIC This module has no serialization logic - records arrive pre-serialized
# MAGIC from the paginator as `ApiPage.content` (JSONL strings) and are written
# MAGIC directly to DBFS/ADLS without any further transformation.
# MAGIC
# MAGIC Depends on: `logging_utils`, `types` (must be `%run` first).

# COMMAND ----------
from typing import Iterator

# COMMAND ----------

def stream_pages_to_tmp(
    page_iter: Iterator["ApiPage"],
    tmp_path: str,
    dbutils,
    log: "ContextLogger",
) -> int:
    """
    Write each ApiPage to a numbered JSONL file in tmp_path.

    No serialization happens here - ApiPage.content is already a JSONL string
    produced by the paginator. This function is a pure I/O operation.

    Args:
        page_iter: Generator from PAGINATION_DISPATCH - yields ApiPage objects.
        tmp_path:  Staging directory in ADLS/DBFS.
        dbutils:   Databricks dbutils (injected by caller).
        log:       ContextLogger from logging_utils.

    Returns:
        Total number of records written across all pages.
    """
    records_written = 0

    for page_num, page in enumerate(page_iter):
        dbutils.fs.put(
            f"{tmp_path}page_{page_num:05d}.jsonl",
            page.content,
            overwrite=True,
        )
        records_written += page.count
        log.debug(
            f"Page written page={page_num}"
            f" size={page.count}"
            f" cumulative={records_written}"
        )

    log.info(f"All pages streamed to tmp path={tmp_path} records={records_written}")
    return records_written


def finalize_to_parquet(
    spark,
    tmp_path: str,
    landing_path: str,
    log: "ContextLogger",
) -> int:
    """
    Read all JSONL pages from tmp_path and write as a single Parquet dataset.

    Spark infers schema across all pages in one pass, so column names and
    types are consistent even when individual pages have different field sets
    (e.g. optional fields that appear on only some records).

    Args:
        spark:        Active SparkSession.
        tmp_path:     Staging directory written by stream_pages_to_tmp().
        landing_path: Final Parquet destination in the landing zone.
        log:          ContextLogger from logging_utils.

    Returns:
        Exact row count written to landing_path.
    """
    log.info(f"Converting JSONL to Parquet src={tmp_path} dst={landing_path}")
    rdf = spark.read.json(tmp_path)
    # persist() so that write and count() share one scan of tmp_path instead of two
    rdf.persist()
    try:
        rdf.write.mode("overwrite").parquet(landing_path)
        count = rdf.count()
    finally:
        rdf.unpersist()
    log.info(f"Parquet write complete records_loaded={count} path={landing_path}")
    return count


def cleanup_tmp(
    tmp_path: str,
    dbutils,
    log: "ContextLogger",
) -> None:
    """
    Remove the staging directory. Always call this in a finally block.

    Swallows exceptions so a cleanup failure does not mask the original error.
    """
    try:
        dbutils.fs.rm(tmp_path, recurse=True)
        log.debug(f"Tmp path removed path={tmp_path}")
    except Exception as exc:
        log.warning(f"Could not remove tmp path path={tmp_path} reason={exc}")