# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · Entity Extractor - Enterprise Ingestion Framework
# MAGIC Single-entity extraction driver. All logic lives in `_lib/` modules;
# MAGIC this notebook is pure orchestration - config load, audit lifecycle,
# MAGIC and the try/except/finally that ties the modules together.
# MAGIC
# MAGIC **Run by:** `00_Orchestrator_Ingestion` (via `dbutils.notebook.run`)
# MAGIC or interactively with `entity_id` widget set.

# COMMAND ----------
# MAGIC %run ./_lib/logging_utils

# COMMAND ----------
# MAGIC %run ./_lib/types

# COMMAND ----------
# MAGIC %run ./_lib/http_client

# COMMAND ----------
# MAGIC %run ./_lib/auth

# COMMAND ----------
# MAGIC %run ./_lib/paginators

# COMMAND ----------
# MAGIC %run ./_lib/audit

# COMMAND ----------
# MAGIC %run ./_lib/landing_writer

# COMMAND ----------
# -- WIDGETS -----------------------------------------------------------------
dbutils.widgets.text(   "entity_id",    "", "Entity ID e.g. SNOW_INCIDENTS")
dbutils.widgets.text(   "batch_run_id", "", "Orchestrator batch run ID (audit correlation)")
dbutils.widgets.dropdown("log_level",    "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"])

# COMMAND ----------
# -- IMPORTS -----------------------------------------------------------------
import json
import uuid
from datetime import datetime, timezone

# COMMAND ----------
# -- PARAMETERS --------------------------------------------------------------
entity_id    = dbutils.widgets.get("entity_id").strip()
batch_run_id = dbutils.widgets.get("batch_run_id").strip()
log_level    = dbutils.widgets.get("log_level")

assert entity_id, "entity_id widget must not be empty"

RUN_ID  = str(uuid.uuid4())
NB_NAME = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)

_base_log = _setup_logger("ingestion.extractor", log_level)
log = ContextLogger(_base_log, {
    "run_id": RUN_ID,
    "entity": entity_id,
    "batch":  batch_run_id or None,
})

log.info("Entity extractor started")

# COMMAND ----------
# -- CONFIG LOAD -------------------------------------------------------------

cfg = spark.sql(f"""
    SELECT
        e.entity_id,
        e.entity_name,
        e.endpoint_url,
        e.http_method,
        e.pagination_type,
        e.watermark_column_name,
        e.watermark_column_value,
        s.source_id,
        s.source_name,
        s.base_url,
        s.auth_type
    FROM ingestion_entity_config e
    INNER JOIN ingestion_source_config s ON e.source_id = s.source_id
    WHERE e.entity_id   = '{entity_id}'
      AND e.active_flag = TRUE
      AND s.is_active   = TRUE
""").first()

if cfg is None:
    raise ValueError(
        f"No active config found for entity_id='{entity_id}'. "
        "Verify ingestion_entity_config.active_flag and ingestion_source_config.is_active."
    )

log.info(
    f"Config loaded source={cfg.source_name}"
    f" auth={cfg.auth_type}"
    f" pagination={cfg.pagination_type}"
    f" watermark={cfg.watermark_column_value}"
)

# COMMAND ----------
# -- CONFIG VALIDATION -------------------------------------------------------
# Fail fast before any network call or audit write.

_REQUIRED_CFG_FIELDS = [
    "entity_id", "endpoint_url", "http_method", "pagination_type",
    "landing_zone_path", "watermark_column_name", "base_url", "auth_type",
]

missing = [f for f in _REQUIRED_CFG_FIELDS if not getattr(cfg, f, None)]
if missing:
    raise ValueError(f"Config missing required fields: {missing}")

if cfg.pagination_type not in PAGINATION_DISPATCH:
    raise ValueError(
        f"pagination_type='{cfg.pagination_type}' not in PAGINATION_DISPATCH. "
        f"Valid: {sorted(PAGINATION_DISPATCH.keys())}"
    )

log.info("Config validation passed")

# COMMAND ----------
# -- INFRASTRUCTURE SETUP ----------------------------------------------------

http_session = build_http_session()
log.debug("HTTP session ready")

auth_headers = get_auth_headers(cfg.auth_type, cfg.source_name, log)
auth_headers["Accept"] = "application/json"
log.info(f"Auth ready auth_type={cfg.auth_type}")

# COMMAND ----------
# -- OPEN RUN ----------------------------------------------------------------
# Write RUNNING before the first API call. Any notebook that is killed,
# OOM'd, or timed out will leave a RUNNING row in the audit table, which
# makes stuck runs visible without any external monitoring:
#
# SELECT * FROM ingestion_audit_log
# WHERE status = 'RUNNING'
#   AND run_start_time < current_timestamp() - INTERVAL 2 HOURS

run_start      = datetime.now(timezone.utc)
remarks_prefix = f"batch_run_id={batch_run_id} | " if batch_run_id else ""

upsert_audit(
    spark, RUN_ID,
    build_audit_payload(
        run_id    = RUN_ID,
        cfg       = cfg,
        nb_name   = NB_NAME,
        run_start = run_start,
        status    = "RUNNING",
        remarks   = f"{remarks_prefix}Extraction started",
    ),
    log,
)

log.info("Audit opened status=RUNNING")

# COMMAND ----------
# -- EXTRACT -> LAND -> CLOSE AUDIT -> ADVANCE WATERMARK ---------------------
#
# Separation of concerns:
#   paginators.py      yields one page at a time (no accumulation)
#   landing_writer.py  writes each page to tmp, then consolidates to Parquet
#   audit.py           opens / closes the audit row
#
# Watermark correctness:
#   Advanced to run_start (not datetime.now() at run end) so any record
#   modified during this extraction window is recaptured on the next run.
#
# exit_payload is initialised before the try block so the finally clause
# never hits a NameError if an exception fires before the assignment.

records_read   = 0
records_loaded = 0
landing_path   = ""
tmp_path       = f"{cfg.landing_zone_path}_tmp_{RUN_ID}/"

exit_payload: dict = {"status": "FAILED", "entity_id": entity_id, "run_id": RUN_ID}

try:
    paginator = PAGINATION_DISPATCH[cfg.pagination_type]

    ts_label     = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    landing_path = f"{cfg.landing_zone_path}extracted_at={ts_label}/"

    log.info(
        f"Extraction starting"
        f" endpoint={cfg.base_url}{cfg.endpoint_url}"
        f" filter={cfg.watermark_column_name}>={cfg.watermark_column_value}"
    )

    # Stream pages -> tmp (one page in driver memory at a time)
    page_iter = paginator(
        http_session,
        cfg.base_url,
        cfg.endpoint_url,
        auth_headers,
        cfg.watermark_column_name,
        str(cfg.watermark_column_value),
        log,
    )
    records_read = stream_pages_to_tmp(page_iter, tmp_path, dbutils, log)

    # Consolidate JSONL pages -> Parquet (Spark reads all pages at once)
    if records_read > 0:
        records_loaded = finalize_to_parquet(spark, tmp_path, landing_path, log)
    else:
        log.info("No records returned - landing zone not written")

    # Close audit: SUCCESS
    run_end = datetime.now(timezone.utc)
    upsert_audit(
        spark, RUN_ID,
        build_audit_payload(
            run_id         = RUN_ID,
            cfg            = cfg,
            nb_name        = NB_NAME,
            run_start      = run_start,
            run_end        = run_end,
            status         = "SUCCESS",
            records_read   = records_read,
            records_loaded = records_loaded,
            records_failed = 0,
            file_path      = landing_path,
            remarks        = f"{remarks_prefix}Loaded {records_loaded} records -> {landing_path}",
        ),
        log,
    )

    log.info(
        f"Audit closed status=SUCCESS"
        f" duration={round((run_end - run_start).total_seconds(), 2)}s"
    )

    # Advance watermark to the maximum value of the watermark column in the
    # data just written to the landing zone. Using the actual data max ensures
    # the next run starts exactly where this run's data ends - no records are
    # skipped and no unnecessary re-fetch of already-landed records.
    # CAST to TIMESTAMP handles sources where the column is a string
    # (e.g. ServiceNow sys_updated_on = "2024-01-15 10:30:00").
    if records_read > 0:
        max_row = (
            spark.read.parquet(landing_path)
            .selectExpr(
                f"max(CAST(`{cfg.watermark_column_name}` AS TIMESTAMP)) AS max_wm"
            )
            .first()
        )
        max_wm = max_row["max_wm"]

        if max_wm is not None:
            wm_ts = max_wm.strftime("%Y-%m-%d %H:%M:%S.%f")
            spark.sql(f"""
                UPDATE ingestion_entity_config
                SET   watermark_column_value = TIMESTAMP '{wm_ts}'
                WHERE entity_id = '{entity_id}'
            """)
            log.info(
                f"Watermark advanced"
                f" column={cfg.watermark_column_name}"
                f" prev={cfg.watermark_column_value}"
                f" new={wm_ts}"
            )
        else:
            log.warning(
                f"Watermark not advanced - max(`{cfg.watermark_column_name}`) is NULL"
                " in extracted data. Verify the column name in ingestion_entity_config."
            )
    else:
        log.info("Watermark not advanced - no records extracted this run")

    exit_payload = {
        "status":         "SUCCESS",
        "entity_id":      entity_id,
        "run_id":         RUN_ID,
        "records_read":   records_read,
        "records_loaded": records_loaded,
        "landing_path":   landing_path,
        "duration_sec":   round((run_end - run_start).total_seconds(), 2),
    }

except Exception as exc:
    run_end = datetime.now(timezone.utc)
    err_msg = str(exc)[:1000]

    upsert_audit(
        spark, RUN_ID,
        build_audit_payload(
            run_id         = RUN_ID,
            cfg            = cfg,
            nb_name        = NB_NAME,
            run_start      = run_start,
            run_end        = run_end,
            status         = "FAILED",
            records_read   = records_read,
            records_loaded = records_loaded,
            records_failed = 1,
            file_path      = landing_path,
            remarks        = f"{remarks_prefix}{err_msg}",
        ),
        log,
    )

    log.error(
        f"Extraction failed"
        f" duration={round((run_end - run_start).total_seconds(), 2)}s"
        f" error={err_msg}"
    )

    exit_payload = {
        "status":    "FAILED",
        "entity_id": entity_id,
        "run_id":    RUN_ID,
        "error":     err_msg,
    }
    # dbutils.notebook.exit() in the finally block takes precedence over this
    # re-raise - the orchestrator receives the FAILED JSON payload rather than
    # an exception. The raise is retained as a fallback for the rare case where
    # dbutils.notebook.exit() itself fails.
    raise

finally:
    cleanup_tmp(tmp_path, dbutils, log)
    dbutils.notebook.exit(json.dumps(exit_payload))