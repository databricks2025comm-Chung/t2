# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/audit
# MAGIC Single-row MERGE helper for `ingestion_audit_log`.
# MAGIC Depends on: `logging_utils` (must be `%run` first).

# COMMAND ----------
from datetime import datetime
from typing import Optional
from pyspark.sql import Row
import re

# COMMAND ----------

def upsert_audit(
    spark,
    run_id: str,
    payload: dict,
    log: "ContextLogger",
) -> None:
    """
    MERGE one audit row into ingestion_audit_log, keyed by run_id.

    Using MERGE (not INSERT) makes every close idempotent - re-running a
    cell after a transient Spark failure won't create duplicate rows.

    Args:
        spark:   Active SparkSession.
        run_id:  Unique identifier for this entity run.
        payload: Dict whose keys match the ingestion_audit_log schema.
        log:     ContextLogger from logging_utils.
    """
    row = Row(
        run_id                 = run_id,
        source_id              = payload.get("source_id"),
        entity_id              = payload.get("entity_id"),
        run_start_time         = payload.get("run_start_time"),
        run_end_time           = payload.get("run_end_time"),
        status                 = payload.get("status"),
        records_read           = payload.get("records_read"),
        records_loaded         = payload.get("records_loaded"),
        records_failed         = payload.get("records_failed"),
        execution_duration_sec = payload.get("execution_duration_sec"),
        notebook_name          = payload.get("notebook_name"),
        remarks                = payload.get("remarks"),
        file_path              = payload.get("file_path"),
    )

    # Each call gets a unique temp-view name derived from run_id.
    # Prevents a race condition if upsert_audit() is ever called from multiple
    # threads within the same Spark session (shared notebook/REPL contexts).
    safe_id   = re.sub(r"[^a-zA-Z0-9]", "_", run_id)
    view_name = f"_audit_stage_{safe_id}"

    try:
        spark.createDataFrame([row]).createOrReplaceTempView(view_name)
        spark.sql(f"""
            MERGE INTO ingestion_audit_log AS tgt
            USING {view_name}              AS src
               ON tgt.run_id = src.run_id
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT     *
        """)
    finally:
        # Always drop - keeps the session catalogue clean regardless of outcome.
        spark.catalog.dropTempView(view_name)

    log.debug(f"Audit upserted status={payload.get('status')}")


def build_audit_payload(
    run_id: str,
    cfg,
    nb_name: str,
    run_start: datetime,
    status: str,
    remarks: str,
    run_end:        Optional[datetime] = None,
    records_read:   Optional[int]      = None,
    records_loaded: Optional[int]      = None,
    records_failed: Optional[int]      = None,
    file_path: str                     = "",
) -> dict:
    """
    Build the audit payload dict so callers don't repeat field names.

    Args:
        cfg: Row from the entity+source config join (has source_id, entity_id).
    """
    duration = (
        round((run_end - run_start).total_seconds(), 2)
        if run_end is not None else None
    )

    return {
        "run_id":                 run_id,
        "source_id":              cfg.source_id,
        "entity_id":              cfg.entity_id,
        "run_start_time":         run_start,
        "run_end_time":           run_end,
        "status":                 status,
        "records_read":           records_read,
        "records_loaded":         records_loaded,
        "records_failed":         records_failed,
        "execution_duration_sec": duration,
        "notebook_name":          nb_name,
        "remarks":                remarks,
        "file_path":              file_path,
    }