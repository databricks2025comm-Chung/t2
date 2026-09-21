# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/logging_utils
# MAGIC Shared structured logger used by every notebook in the ingestion framework.
# MAGIC `%run` this before any other _lib module.

# COMMAND ----------
import logging
import time
from typing import Any


def _setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Return a stdout logger with ISO-8601 timestamps and level padding.
    Idempotent — re-running a cell won't add duplicate handlers.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        return logger
    handler   = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-5s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    formatter.converter = time.gmtime   # emit UTC so the trailing Z is not a lie
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    return logger


class ContextLogger(logging.LoggerAdapter):
    """
    Prepends a fixed key=value context block to every log message so that
    run_id, source_id, and entity_id appear on every line without
    the caller having to repeat them.

    Usage:
        base = _setup_logger("ingestion.extractor", log_level)
        log  = ContextLogger(base, {"run_id": RUN_ID, "entity_id": entity_id})
        log.info("started")
        # → 2026-09-09T08:00:00Z [INFO ] ingestion.extractor — [run_id=abc entity=SNOW] started
    """
    def process(self, msg: str, kwargs: Any):
        ctx = " ".join(f"{k}={v}" for k, v in self.extra.items() if v is not None)
        return f"[{ctx}] {msg}", kwargs
