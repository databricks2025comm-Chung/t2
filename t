import json

def _write_chunk(spark, records, path, chunk_num, log):
    lines = [(json.dumps(r, default=str),) for r in records]
    (
        spark.createDataFrame(lines, "value string")   # one string column, no inference
             .coalesce(1)
             .write
             .mode("append")
             .text(path)
    )
    
    
------------------------


import json as _json

def _write_chunk(
    spark,
    records:   list,
    path:      str,
    chunk_num: int,
    log:       "ContextLogger",
) -> None:
    """
    Write `records` as one JSON-Lines text file appended to `path`.

    Each record is serialised with json.dumps and stored in a single
    `value string` column, so Spark never infers a schema from the data.
    Mixed types, all-null fields and empty lists cannot fail the write, and
    records land exactly as the API returned them.
    """
    # Serialise once, outside the retry loop.
    lines = [(_json.dumps(r, default=str),) for r in records]

    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(1, _WRITE_RETRIES + 1):
        try:
            (
                spark.createDataFrame(lines, "value string")
                     .coalesce(1)
                     .write
                     .mode("append")
                     .text(path)
            )
            log.debug(
                f"Chunk written"
                f"  chunk={chunk_num}"
                f"  records={len(records)}"
                f"  path={path}"
            )
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _WRITE_RETRIES:
                sleep = _WRITE_BACKOFF * (2 ** (attempt - 1))
                log.warning(
                    f"Chunk write failed — retrying"
                    f"  attempt={attempt}/{_WRITE_RETRIES}"
                    f"  sleeping={sleep}s"
                    f"  chunk={chunk_num}"
                    f"  error={exc}"
                )
                _time.sleep(sleep)

    raise RuntimeError(
        f"Chunk write failed after {_WRITE_RETRIES} attempts"
        f"  chunk={chunk_num}"
        f"  path={path}"
        f"  error={last_exc}"
    ) from last_exc




-----------------------------





import json as _json

def _build_dataframe(spark, records: list, chunk_num: int, log: "ContextLogger"):
    """
    Try normal schema inference first.  If Spark cannot infer one schema for
    the whole chunk (mixed types, all-null field, empty list), return None
    so the caller falls back to raw JSON text.
    """
    try:
        return spark.createDataFrame(records)
    except Exception as exc:
        log.warning(
            f"Schema inference failed — chunk will be written as raw JSON text"
            f"  chunk={chunk_num}  records={len(records)}  error={str(exc)[:300]}"
        )
        return None


def _write_chunk(
    spark,
    records:   list,
    path:      str,
    chunk_num: int,
    log:       "ContextLogger",
) -> None:
    """
    Write `records` as a single-partition file appended to `path`.

    1. Normal path: createDataFrame(records) -> .json(path)
    2. If inference fails: each record is serialised with json.dumps and
       written as one line of text.  Same JSON-Lines content, no type
       inference, so every record still lands.

    Both file types sit in the same directory; spark.read.json() reads
    both and widens types on the read side.
    """
    # Inference happens here, before any write, so a failure leaves nothing behind.
    df = _build_dataframe(spark, records, chunk_num, log)
    if df is None:
        lines = [(_json.dumps(r, default=str),) for r in records]
        df    = spark.createDataFrame(lines, "value string")
        use_text = True
    else:
        use_text = False

    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(1, _WRITE_RETRIES + 1):
        try:
            writer = df.coalesce(1).write.mode("append")
            writer.text(path) if use_text else writer.json(path)
            log.debug(
                f"Chunk written"
                f"  chunk={chunk_num}"
                f"  records={len(records)}"
                f"  format={'text' if use_text else 'json'}"
                f"  path={path}"
            )
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _WRITE_RETRIES:
                sleep = _WRITE_BACKOFF * (2 ** (attempt - 1))
                log.warning(
                    f"Chunk write failed — retrying"
                    f"  attempt={attempt}/{_WRITE_RETRIES}"
                    f"  sleeping={sleep}s"
                    f"  chunk={chunk_num}"
                    f"  error={exc}"
                )
                _time.sleep(sleep)

    raise RuntimeError(
        f"Chunk write failed after {_WRITE_RETRIES} attempts"
        f"  chunk={chunk_num}"
        f"  path={path}"
        f"  error={last_exc}"
    ) from last_exc




----

path = "abfss://landing@datalake.dfs.core.windows.net/_test/autoloader_check/"

# one .json (via the normal writer) and one .txt (via the text fallback)
spark.createDataFrame([{"id": 1, "storyPoints": 3, "labels": ["a"]}]).coalesce(1).write.mode("append").json(path)
spark.createDataFrame([('{"id": 2, "storyPoints": 2.5, "assignee": {"name": "bob"}}',)], "value string") \
     .coalesce(1).write.mode("append").text(path)

df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", path + "_schema")
        .load(path))

(df.writeStream
   .trigger(availableNow=True)
   .option("checkpointLocation", path + "_chk")
   .toTable("main.default.autoloader_check"))   # change to a scratch table you can use

spark.table("main.default.autoloader_check").printSchema()
display(spark.table("main.default.autoloader_check"))   # expect 2 rows


--------


    
    
    
    from datetime import datetime
from typing import Optional
import re


def upsert_audit(
    spark,
    run_id:  str,
    payload: dict,
    log:     "ContextLogger",
) -> None:
    """
    MERGE one audit row into ingestion_audit_log, keyed by run_id.

    The staging DataFrame is built with the target table's own schema, so
    None values never trigger type inference and every column gets the
    exact type the table expects.

    Args:
        spark:   Active SparkSession.
        run_id:  Unique identifier for this entity run.
        payload: Dict whose keys match the ingestion_audit_log columns.
        log:     ContextLogger from logging_utils.
    """
    # Metadata-only read: no data is scanned.
    tbl_schema = spark.table("ingestion_audit_log").schema

    # One value per table column, in table order. Missing keys become None.
    values = tuple(
        run_id if f.name == "run_id" else payload.get(f.name)
        for f in tbl_schema.fields
    )

    # Unique temp-view name per call avoids collisions if upsert_audit()
    # is ever called from multiple threads in the same Spark session.
    safe_id   = re.sub(r"[^a-zA-Z0-9]", "_", run_id)
    view_name = f"_audit_stage_{safe_id}"

    try:
        spark.createDataFrame([values], schema=tbl_schema) \
             .createOrReplaceTempView(view_name)
        spark.sql(f"""
            MERGE INTO ingestion_audit_log AS tgt
            USING {view_name}              AS src
              ON  tgt.run_id = src.run_id
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT  *
        """)
    finally:
        spark.catalog.dropTempView(view_name)

    log.debug(f"Audit upserted  status={payload.get('status')}")
    


-----------------------------------------------------------------------------

payload = {**payload, "run_id": run_id}   # argument wins; guarantees the merge key exists

values = tuple(payload.get(f.name) for f in tbl_schema.fields)

----------------





PAYLOAD_KEYS = {
    "run_id", "source_id", "entity_id", "run_start_time", "run_end_time",
    "status", "records_read", "records_loaded", "records_failed",
    "execution_duration_sec", "notebook_name", "remarks", "file_path",
}

desc = spark.sql("DESCRIBE TABLE ingestion_audit_log").collect()
# Skip partition and metadata rows that appear after the first blank/"#" row.
table_cols = []
for r in desc:
    if not r.col_name or r.col_name.startswith("#"):
        break
    table_cols.append(r.col_name)

print("Table columns :", table_cols)
print("Extra in table:", sorted(set(table_cols) - PAYLOAD_KEYS))
print("Missing in table:", sorted(PAYLOAD_KEYS - set(table_cols)))

display(spark.sql("DESCRIBE TABLE ingestion_audit_log"))



---------------------------------------------------------------------------


    
    
    
    
    
    
    
    
