# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 - Entity Extractor - Enterprise Ingestion Framework
# MAGIC Single-entity extraction driver. Accepts `source_id` (INT) and
# MAGIC `entity_id` (STRING) as explicit widget parameters, loads the combined
# MAGIC config row, runs the full extract -> land -> audit -> watermark cycle.
# MAGIC 
# MAGIC **Invoked by:** Databricks Workflow (one workflow per entity) or run
# MAGIC interactively by setting both widgets.
# MAGIC 
# MAGIC Widget parameters (match ingestion_entity_config / ingestion_source_config DDL):
# MAGIC source_id INT - ingestion_source_config.source_id
# MAGIC entity_id STRING - ingestion_entity_config.entity_id
# MAGIC run_id STRING - passed as {{job.run_id}} by the workflow; auto-generated UUID when empty (interactive)

# COMMAND --------
# MAGIC %run ./_lib/logging_utils

# COMMAND --------
# MAGIC %run ./_lib/http_client

# COMMAND --------
# MAGIC %run ./_lib/auth

# COMMAND --------
# MAGIC %run ./_lib/paginators

# COMMAND --------
# MAGIC %run ./_lib/audit

# COMMAND --------
# MAGIC %run ./_lib/landing_writer

# COMMAND --------
# - IMPORTS
import uuid
from datetime import datetime, timezone

# COMMAND --------
# - WIDGETS
dbutils.widgets.text(   "source_id", "",    "Source ID (INT) e.g. 5")
dbutils.widgets.text(   "entity_id", "",    "Entity ID (STRING) e.g. SNOW_INCIDENTS")
dbutils.widgets.text(   "run_id",    "",    "Run ID (set by {{job.run_id}}; UUID auto-generated when empty)")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"])

# COMMAND --------
# - PARAMETERS & EARLY SETUP
# RUN_ID and the base logger are created first so that validation failures
# can be written to ingestion_audit_log before the notebook exits.

_raw_source_id = dbutils.widgets.get("source_id").strip()
entity_id      = dbutils.widgets.get("entity_id").strip()
_wgt_run_id    = dbutils.widgets.get("run_id").strip()
log_level      = dbutils.widgets.get("log_level")

# Pre-initialise so the ContextLogger line after the validation block never
# hits NameError if dbutils.notebook.exit() somehow doesn't stop execution.
source_id = -1

# Use the workflow-supplied run_id when available; fall back to a UUID for
# interactive / ad-hoc runs so the audit log is always populated.
RUN_ID = _wgt_run_id if _wgt_run_id else str(uuid.uuid4())

NB_NAME = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)

_base_log = _setup_logger("ingestion.extractor", log_level)

# COMMAND --------
# - WIDGET VALIDATION
# Runs before config is loaded. Any failure here writes a minimal FAILED audit
# row so the broken run is visible in ingestion_audit_log alongside healthy runs.

try:
    if not entity_id:
        raise ValueError("entity_id widget must not be empty")
    
    try:
        source_id = int(_raw_source_id)
    except ValueError:
        raise ValueError(
            f"source_id widget must be an integer matching "
            f"ingestion_source_config.source_id, "
            f"got '{_raw_source_id}'"
        )
    
    if source_id <= 0:
        raise ValueError(
            f"source_id must be a positive integer, got {source_id}"
        )

except Exception as _val_exc:
    # cfg is not available yet - build a minimal stub for the audit helper.
    _run_start = datetime.now(timezone.utc)
    
    class _Mincfg:
        try:
            source_id = int(_raw_source_id)
        except (ValueError, TypeError):
            source_id = -1
        entity_id = entity_id or "UNKNOWN"
    
    upsert_audit(
        spark, RUN_ID,
        build_audit_payload(
            run_id     = RUN_ID,
            cfg        = _Mincfg(),
            nb_name    = NB_NAME,
            run_start  = _run_start,
            run_end    = _run_start,
            status     = "FAILED",
            remarks    = f"Widget validation failed: {_val_exc}",
        ),
        _base_log,
    )
    raise

log = ContextLogger(_base_log, {
    "run_id":    RUN_ID,
    "source_id": source_id,
    "entity_id": entity_id,
})

log.info("Entity extractor started")

# COMMAND --------
# - CONFIG LOAD
# Filter on BOTH source_id (INT, unquoted) and entity_id (STRING, quoted).

try:
    cfg = spark.sql(f"""
        SELECT
            e.entity_id,
            e.entity_name,
            e.endpoint_url,
            e.http_method,
            e.pagination_type,
            e.landing_zone_path,
            e.watermark_column_name,
            e.watermark_column_value,
            s.source_id,
            s.source_name,
            s.base_url,
            s.auth_type
        FROM ingestion_entity_config e
        INNER JOIN ingestion_source_config s
          ON e.source_id = s.source_id
        WHERE e.entity_id = '{entity_id}'
          AND e.source_id = {source_id}
          AND e.active_flag = TRUE
          AND s.is_active = TRUE
    """).first()
    
    if cfg is None:
        raise ValueError(
            f"No active config for source_id={source_id}, entity_id='{entity_id}'. "
            f"Check ingestion_entity_config.active_flag and ingestion_source_config.is_active."
        )

except Exception as _cfg_exc:
    _cfg_run_start = datetime.now(timezone.utc)
    
    class _CfgStub:
        source_id = source_id
        entity_id = entity_id
    
    upsert_audit(
        spark, RUN_ID,
        build_audit_payload(
            run_id     = RUN_ID,
            cfg        = _CfgStub(),
            nb_name    = NB_NAME,
            run_start  = _cfg_run_start,
            run_end    = _cfg_run_start,
            status     = "FAILED",
            remarks    = f"Config load failed: {_cfg_exc}",
        ),
        log,
    )
    raise

log.info(
    f"Config loaded"
    f" source={cfg.source_name}"
    f" auth={cfg.auth_type}"
    f" pagination={cfg.pagination_type}"
    f" watermark_col={cfg.watermark_column_name}"
    f" watermark_val={cfg.watermark_column_value}"
)

# COMMAND --------
# - OPEN AUDIT ROW (RUNNING)
# Written before any network call so that failures in auth or HTTP setup
# (secret missing, connection refused) also close as FAILED in the audit table.
# Any run killed mid-flight (OOM, cluster eviction, timeout) leaves a visible
# RUNNING row detectable with:
# 
# SELECT * FROM ingestion_audit_log
# WHERE status = 'RUNNING'
# AND run_start_time < current_timestamp() - INTERVAL 2 HOURS

run_start = datetime.now(timezone.utc)

upsert_audit(
    spark, RUN_ID,
    build_audit_payload(
        run_id     = RUN_ID,
        cfg        = cfg,
        nb_name    = NB_NAME,
        run_start  = run_start,
        status     = "RUNNING",
        remarks    = "Extraction started",
    ),
    log,
)
log.info("Audit row opened status=RUNNING")

# COMMAND --------
# - EXTRACT -> LAND -> AUDIT -> WATERMARK
# Module responsibilities:
# paginators.py yields list[dict] one page at a time
# landing_writer.py serialises each page to JSONL in tmp, then consolidates
# to Parquet (append mode - bronze is an immutable archive)
# audit.py MERGE-based upsert keeps every close idempotent
# 
# Landing path layout:
# <landing_zone_path>/extracted_at=<YYYYMMDDTHHMMSSZ>/<RUN_ID>/
# RUN_ID sub-folder guarantees uniqueness per run so append mode is safe
# on retries.
# 
# exit_payload pre-initialised so the finally clause never hits NameError
# if an exception fires before the try block assigns it.

records_read   = 0
records_loaded = 0
landing_path   = ""

try:
    # - CONFIG VALIDATION
    # Inside the try block so a bad pagination_type or missing field also
    # closes the RUNNING audit row as FAILED rather than leaving it open.
    _REQUIRED = [
        "entity_id", "endpoint_url", "http_method", "pagination_type",
        "landing_zone_path", "watermark_column_name", "base_url", "auth_type",
    ]
    missing = [f for f in _REQUIRED if not getattr(cfg, f, None)]
    if missing:
        raise ValueError(f"Config row is missing required fields: {missing}")
    
    if cfg.pagination_type not in PAGINATION_DISPATCH:
        raise ValueError(
            f"pagination_type='{cfg.pagination_type}' is not registered. "
            f"Valid values: {sorted(PAGINATION_DISPATCH)}"
        )
    
    log.info("Config validation passed")
    
    # - INFRASTRUCTURE SETUP
    # Placed inside the try block so auth failures (secret missing) and HTTP
    # session errors are caught and written to the audit table as FAILED.
    http_session = build_http_session()
    log.debug("HTTP session built")
    
    auth_headers = get_auth_headers(cfg.auth_type, cfg.source_name, log)
    auth_headers["Accept"] = "application/json"
    log.info(f"Auth headers ready auth_type={cfg.auth_type}")
    
    paginator    = PAGINATION_DISPATCH[cfg.pagination_type]
    ts_label     = run_start.strftime("%Y%m%dT%H%M%SZ")
    landing_path = f"{cfg.landing_zone_path}extracted_at={ts_label}/{RUN_ID}/"
    
    log.info(
        f"Extraction starting"
        f" url={cfg.base_url}{cfg.endpoint_url}"
        f" {cfg.watermark_column_name}>={cfg.watermark_column_value}"
    )
    
    # - PAGINATE & WRITE TO LANDING
    # token_refresher is a zero-arg callable that fetches a fresh auth header
    # dict from Databricks Secrets. safe_get() calls it on 401 and mutates
    # auth_headers in-place so the next paginator page already uses the new token.
    token_refresher = lambda: get_auth_headers(cfg.auth_type, cfg.source_name, log)
    
    page_iter = paginator(
        http_session,
        cfg.base_url,
        cfg.endpoint_url,
        auth_headers,
        cfg.watermark_column_name,
        str(cfg.watermark_column_value),
        log,
        token_refresher=token_refresher,
    )
    records_loaded = write_pages_to_parquet(page_iter, landing_path, spark, log)
    records_read   = records_loaded
    
    # - ADVANCE WATERMARK
    # Watermark is advanced BEFORE the audit SUCCESS row is written so the
    # final audit remarks always reflect the true outcome of this step.
    # 
    # On watermark failure we do NOT raise - the landing write is already
    # committed and raising here would trigger a Workflow retry that re-lands
    # the same records as duplicates downstream. Instead we log CRITICAL and
    # embed the manual-fix SQL in the audit remarks so ops can query:
    # 
    # SELECT run_id, remarks FROM ingestion_audit_log
    # WHERE remarks LIKE '%CRITICAL: watermark%'
    audit_remarks = f"Loaded {records_loaded} records -> {landing_path}"
    
    if records_read > 0:
        wm_ts = None
        try:
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
                    WHERE source_id = {source_id}
                      AND entity_id = '{entity_id}'
                """)
                log.info(
                    f"Watermark advanced"
                    f" column={cfg.watermark_column_name}"
                    f" prev={cfg.watermark_column_value}"  # cfg is a snapshot - intentionally the old value
                    f" new={wm_ts}"
                )
            else:
                log.warning(
                    f"Watermark not advanced - "
                    f"max(`{cfg.watermark_column_name}`) is NULL in landed data. "
                    f"Verify watermark_column_name in ingestion_entity_config."
                )
                audit_remarks += " | WARNING: watermark column returned NULL"
        except Exception as wm_exc:
            wm_err = str(wm_exc)[:500]
            manual_fix = (
                f"UPDATE ingestion_entity_config "
                f"SET watermark_column_value = TIMESTAMP '{wm_ts}' "
                f"WHERE source_id = {source_id} AND entity_id = '{entity_id}'"
            ) if wm_ts else "wm_ts not computed - check watermark_column_name in config"
            log.critical(
                f"WATERMARK UPDATE FAILED - landing data committed but watermark NOT advanced. "
                f"Next run will re-fetch already-landed records. "
                f"Manual fix: {manual_fix}. "
                f"error={wm_err}"
            )
            audit_remarks += (
                f" | CRITICAL: watermark update failed - manual fix required: "
                f"{manual_fix}. error={wm_err[:200]}"
            )
    else:
        log.info("Watermark unchanged - no records extracted this run")
    
    # - CLOSE AUDIT: SUCCESS
    run_end = datetime.now(timezone.utc)
    upsert_audit(
        spark, RUN_ID,
        build_audit_payload(
            run_id     = RUN_ID,
            cfg        = cfg,
            nb_name    = NB_NAME,
            run_start  = run_start,
            run_end    = run_end,
            status     = "SUCCESS",
            records_read     = records_read,
            records_loaded = records_loaded,
            records_failed = 0,
            file_path     = landing_path,
            remarks     = audit_remarks,
        ),
        log,
    )
    log.info(
        f"Audit closed status=SUCCESS"
        f" records_read={records_read}"
        f" records_loaded={records_loaded}"
        f" duration={round((run_end - run_start).total_seconds(), 2)}s"
    )
except Exception as exc:
    run_end = datetime.now(timezone.utc)
    err_msg = str(exc)[:1000]
    
    upsert_audit(
        spark, RUN_ID,
        build_audit_payload(
            run_id     = RUN_ID,
            cfg        = cfg,
            nb_name    = NB_NAME,
            run_start  = run_start,
            run_end    = run_end,
            status     = "FAILED",
            records_read     = records_read,
            records_loaded = records_loaded,
            records_failed = 1,
            file_path     = landing_path,
            remarks     = err_msg,
        ),
        log,
    )
    log.error(
        f"Extraction failed"
        f" duration={round((run_end - run_start).total_seconds(), 2)}s"
        f" error={err_msg}"
    )
    raise