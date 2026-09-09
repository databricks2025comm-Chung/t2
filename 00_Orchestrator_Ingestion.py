  # Databricks notebook source
# MAGIC %md
# MAGIC ## 00 . Orchestrator - Enterprise Ingestion Framework
# MAGIC #Reads active entities from config tables and dispatches each to
# MAGIC #`01_Entity_Extractor` in **parallel** (ThreadPoolExecutor) or **sequentially**.
# MAGIC #All extraction detail and audit logging is handled by the child notebook.

# COMMAND ----------
# MAGIC %run ./_lib/logging_utils

# COMMAND -----------
# MAGIC %run ./_lib/audit

# COMMAND ----------
# -- WIDGETS -----------------------------------------------------------------
dbutils.widgets.text(    "source_ids",          "",
                         "Source IDs (comma-separated; blank = all active)")
dbutils.widgets.dropdown("execution_mode",      "parallel", ["parallel", "sequential"])
dbutils.widgets.text(    "max_workers",         "5",
                         "Max total parallel threads across all sources")
dbutils.widgets.text(    "per_source_max_workers", "3",
                         "Max concurrent notebooks per source (prevents per-API rate-limit storms)")
dbutils.widgets.text(    "entity_nb_path",      "/Ingestion/01_Entity_Extractor",
                         "Absolute path to entity-extractor notebook")
dbutils.widgets.text(    "notebook_timeout_sec", "3600", "Per-entity timeout (sec)")
dbutils.widgets.dropdown("log_level",           "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"])

# COMMAND ----------
# -- IMPORTS -----------------------------------------------------------------
import json
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore

# COMMAND ----------
# -- PARAMETERS --------------------------------------------------------------
source_ids_param   = dbutils.widgets.get("source_ids").strip()
execution_mode     = dbutils.widgets.get("execution_mode")
max_workers        = int(dbutils.widgets.get("max_workers"))
per_source_workers = int(dbutils.widgets.get("per_source_max_workers"))
entity_nb_path     = dbutils.widgets.get("entity_nb_path")
nb_timeout         = int(dbutils.widgets.get("notebook_timeout_sec"))
log_level          = dbutils.widgets.get("log_level")

BATCH_RUN_ID = str(uuid.uuid4()) # ties every child run back to this orchestration
batch_run_start = datetime.now(timezone.utc)
NB_NAME_ORCH = (
  dbutils.notebook.entry_point
  .getDbutils().notebook().getContext()
  .notebookPath().get()
)

_base_log = _setup_logger("ingestion.orchestrator", log_level)
log = ContextLogger(_base_log, {"batch_run_id": BATCH_RUN_ID, "mode": execution_mode})

log.info("Orchestrator started")

# COMMAND ----------
# -- LOAD ACTIVE ENTITIES ----------------------------------------------------

source_filter = (
    f"AND e.source_id IN ({source_ids_param})"
    if source_ids_param else ""
)

entities_df = spark.sql(f"""
    SELECT
        e.entity_id,
        e.entity_name,
        s.source_id,
        s.source_name
    FROM ingestion_entity_config e
    INNER JOIN ingestion_source_config s ON e.source_id = s.source_id
    WHERE e.active_flag = TRUE
      AND s.is_active   = TRUE
    {source_filter}
    ORDER BY e.source_id, e.entity_id
""")

entities: list[tuple[str, str, int, str]] = [
    (row.entity_id, row.entity_name, row.source_id, row.source_name)
    for row in entities_df.collect()
]

if not entities:
    log.warning("No active entities found - check config tables and source_ids filter")
    dbutils.notebook.exit(json.dumps({"total": 0, "succeeded": 0, "failed": 0, "details": []}))

log.info(f"Entities queued: {len(entities)}  per_source_limit={per_source_workers}")
for eid, ename, sid, sname in entities:
    log.debug(f" source_id={sid} {sname}/{ename} ({eid})")

# COMMAND -----------
# - OPEN BATCH AUDIT --------------------------------------------------------------------------
# Written before dispatch so that a batch killed mid-run (OOM, timeout, manual
# cancel) leaves a RUNNING row visible in ingestion_audit_log - same sentinel
# pattern used by 01_Entity_Extractor for stuck individual runs:
#
# SELECT * FROM ingestion_audit_log
# WHERE status = 'RUNNING'
#   AND entity_id = 'ORCHESTRATOR'
#   AND run_start_time < current_timestamp() - INTERVAL 2 HOURS

upsert_audit(spark, BATCH_RUN_ID, {
  "run_id":               BATCH_RUN_ID,
  "source_id":            None,
  "entity_id":            "ORCHESTRATOR",
  "run_start_time":       batch_run_start,
  "run_end_time":         None,
  "status":               "RUNNING",
  "records_read":         None,
  "records_loaded":       None,
  "records_failed":       None,
  "execution_duration_sec": None,
  "notebook_name":        NB_NAME_ORCH,
  "remarks":              (
      f"Batch started - {len(entities)} entities queued"
      f" mode={execution_mode}"
      f" source_filter={source_ids_param or 'all'}"
  ),
  "file_path": "",
}, log)
log.info("Batch audit opened status=RUNNING")

# COMMAND ----------
# -- PER-SOURCE SEMAPHORE REGISTRY -------------------------------------------
# Goal: allow full parallelism *across* different API sources while capping
# concurrent calls *within* the same source to avoid rate-limit storms.
# Example with max_workers=6, per_source_workers=3, sources=ServiceNow+GitHub:
#   ServiceNow entities: max 3 run at once -> respects SN API rate limit
#   GitHub entities:    max 3 run at once -> respects GH API rate limit
#   Total concurrent:   up to 6           -> bounded by ThreadPoolExecutor
#
# The Lock is needed because multiple threads may race to create a semaphore
# for the same source_id at startup.

_semaphore_registry: dict[int, Semaphore] = {}
_registry_lock      = Lock()


def _get_source_semaphore(source_id: int) -> Semaphore:
    """Return the shared Semaphore for source_id, creating it once if needed."""
    with _registry_lock:
        if source_id not in _semaphore_registry:
            _semaphore_registry[source_id] = Semaphore(per_source_workers)
            log.debug(
                f"Semaphore created source_id={source_id}"
                f" limit={per_source_workers}"
            )
    return _semaphore_registry[source_id]


# COMMAND ----------
# -- DISPATCH ----------------------------------------------------------------

def _run_entity(entity_id: str, source_id: int, batch_run_id: str) -> dict:
    """
    Invoke the entity-extractor notebook for one entity.

    Acquires the per-source semaphore before calling dbutils.notebook.run()
    so that at most `per_source_workers` notebooks call the same API at once.
    Always returns a dict - never raises - so a single failure does not abort
    the batch loop.
    """
    with _get_source_semaphore(source_id):
        try:
            raw = dbutils.notebook.run(
                entity_nb_path,
                timeout_seconds=nb_timeout,
                arguments={
                    "entity_id":    entity_id,
                    "batch_run_id": batch_run_id,
                    "log_level":    log_level,
                },
            )
            result: dict = json.loads(raw) if raw else {}
            result.setdefault("entity_id", entity_id)
            result.setdefault("status",    "SUCCESS")
        except Exception as exc:
            result = {
                "entity_id": entity_id,
                "status":    "FAILED",
                "error":     str(exc)[:500],
            }
    return result


def _log_result(res: dict) -> None:
    if res["status"] == "SUCCESS":
        log.info(
            f"entity_id={res['entity_id']} status=SUCCESS"
            f" records={res.get('records_loaded', '?')}"
            f" duration={res.get('duration_sec', '?')}s"
        )
    else:
        log.error(
            f"entity_id={res['entity_id']} status=FAILED"
            f" error={res.get('error', '')[:120]}"
        )


results: list[dict] = []

if execution_mode == "parallel":
    log.info(f"Parallel dispatch max_workers={max_workers} per_source_limit={per_source_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_run_entity, eid, sid, BATCH_RUN_ID): eid
            for eid, _, sid, _ in entities
        }
        for future in as_completed(futures):
            res = future.result()
            _log_result(res)
            results.append(res)
else:
    log.info("Sequential dispatch")
    for eid, _, sid, _ in entities:
        res = _run_entity(eid, sid, BATCH_RUN_ID)
        _log_result(res)
        results.append(res)

# COMMAND ----------
# - SUMMARY + CLOSE BATCH AUDIT ---------------------------------------------------------------

succeeded = [r for r in results if r.get("status") == "SUCCESS"]
failed    = [r for r in results if r.get("status") != "SUCCESS"]

# Aggregate record counts from child notebook exit payloads
total_read    = sum(r.get("records_read",    0) or 0 for r in results)
total_loaded  = sum(r.get("records_loaded",  0) or 0 for r in results)

# PARTIAL: at least one entity succeeded and at least one failed
if   not failed:      batch_status = "SUCCESS"
elif not succeeded:   batch_status = "FAILED"
else:                 batch_status = "PARTIAL"

batch_run_end = datetime.now(timezone.utc)
batch_duration = round((batch_run_end - batch_run_start).total_seconds(), 2)

upsert_audit(spark, BATCH_RUN_ID, {
  "run_id":                 BATCH_RUN_ID,
  "source_id":              None,
  "entity_id":              "ORCHESTRATOR",
  "run_start_time":         batch_run_start,
  "run_end_time":           batch_run_end,
  "status":                 batch_status,
  "records_read":           total_read,
  "records_loaded":         total_loaded,
  "records_failed":         len(failed),
  "execution_duration_sec": batch_duration,
  "notebook_name":          NB_NAME_ORCH,
  "remarks":                (
          f"{len(succeeded)}/{len(results)} entities succeeded"
          + (
              f"\n failed_entities={[r['entity_id'] for r in failed]}"
              if failed else ""
          )
  ),
  "file_path": "",
}, log)

log.info(
  f"Batch complete"
  f" status={batch_status}"
  f" total={len(results)}"
  f" succeeded={len(succeeded)}"
  f" failed={len(failed)}"
  f" records_loaded={total_loaded}"
  f" duration={batch_duration}s"
)

dbutils.notebook.exit(json.dumps({
  "batch_run_id":   BATCH_RUN_ID,
  "status":         batch_status,
  "total":          len(results),
  "succeeded":      len(succeeded),
  "failed":         len(failed),
  "records_read":   total_read,
  "records_loaded": total_loaded,
  "duration_sec":   batch_duration,
  "details":        results,
}))