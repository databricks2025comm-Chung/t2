# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/landing_writer
# MAGIC Buffers paginated API records in driver memory, creates one DataFrame,
# MAGIC and writes a single Parquet append to the landing zone.
# MAGIC 
# MAGIC Optimised for incremental (watermark-filtered) loads where the per-run
# MAGIC record count is bounded and fits comfortably in driver memory.
# MAGIC 
# MAGIC Depends on: `logging_utils` (must be `%run` first).

# COMMAND --------

def write_pages_to_parquet(
    page_iter:  "Iterator[list[dict]]",
    landing_path: str,
    spark,
    log:        "ContextLogger",
) -> int:
    """
    Collect all pages into driver memory, create one DataFrame, write Parquet.

    Eliminates the tmp JSONL step - no intermediate ADLS writes or reads back.
    Count comes from len() so no extra Spark scan is needed.

    Args:
        page_iter: Generator from a PAGINATION_DISPATCH handler.
        landing_path: Final Parquet destination in the landing zone.
        spark: Active SparkSession.
        log: ContextLogger from logging_utils.

    Returns:
        Total number of records written.
    """
    all_records = []

    for page_num, batch in enumerate(page_iter):
        all_records.extend(batch)
        log.debug(
            f"Page buffered page={page_num}"
            f" size={len(batch)}"
            f" total={len(all_records)}"
        )

    if not all_records:
        log.info("No records returned by API - landing zone not written")
        return 0

    df = spark.createDataFrame(all_records)
    df.write.mode("append").parquet(landing_path)

    count = len(all_records)
    log.info(f"Parquet write complete records={count} path={landing_path}")
    return count