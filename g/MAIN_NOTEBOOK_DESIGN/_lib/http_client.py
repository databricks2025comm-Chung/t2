# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/http_client
# MAGIC Shared HTTP session factory and rate-limit-aware GET/POST helpers.
# MAGIC Depends on: `logging_utils` (must be `%run` first).

# COMMAND ----------
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
import urllib3
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# urllib3 2.x renamed method_whitelist → allowed_methods.
# Pick the right kwarg name so the code works on all Databricks runtimes.
_RETRY_METHODS_KWARG = (
    "allowed_methods" if int(urllib3.__version__.split(".")[0]) >= 2
    else "method_whitelist"
)

# ── Constants ──────────────────────────────────────────────────────────────────
TIMEOUT        = (5, 60)  # (connect_timeout_sec, read_timeout_sec)
MAX_RETRIES    = 3        # urllib3 automatic retries for transient 5xx / network errors
BACKOFF_FACTOR = 2.0      # sleep = backoff_factor * (2 ** (attempt - 1))  → 2, 4, 8 s
MAX_429_WAITS  = 3        # max request attempts for 429 responses; Retry-After is honoured between each attempt (max_waits - 1 sleeps total)

# 5xx codes retried automatically by urllib3; 429 is handled manually so that
# the Retry-After response header is respected instead of being ignored.
_RETRY_ON_STATUS: frozenset[int] = frozenset({500, 502, 503, 504})


def _parse_retry_after(resp: "Response", default: int = 60) -> int:
    """
    Parse the Retry-After header from a 429 response.

    The HTTP spec allows two formats:
      - Seconds:   "30"
      - HTTP-date: "Thu, 16 Sep 2026 10:00:00 GMT"
    Returns seconds to wait as an int; falls back to `default` on any parse error.
    """
    value = resp.headers.get("Retry-After", "")
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        wait = int((retry_at - datetime.now(timezone.utc)).total_seconds())
        return max(wait, 1)
    except Exception:
        return default

# COMMAND ----------

def build_http_session(
    retries: int = MAX_RETRIES,
    backoff: float = BACKOFF_FACTOR,
) -> Session:
    """
    Return a requests.Session with automatic retry and connection pooling.

    urllib3 handles transient 5xx errors with exponential back-off.
    429 rate-limit responses are NOT in status_forcelist — they are handled
    by safe_get() so the Retry-After header value is respected.
    """
    retry = Retry(
        total            = retries,
        backoff_factor   = backoff,
        status_forcelist = _RETRY_ON_STATUS,
        raise_on_status  = False,   # status is inspected in safe_get / safe_post
        **{_RETRY_METHODS_KWARG: {"GET"}},
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


def safe_get(
    session:         Session,
    url:             str,
    headers:         dict,
    params:          dict,
    log:             "ContextLogger",
    max_waits:       int  = MAX_429_WAITS,
    token_refresher       = None,
) -> Response:
    """
    GET the URL with explicit 429 / Retry-After back-off on top of urllib3 retry.

    Args:
        session:         Shared requests.Session (connection-pooled, auto-retries 5xx).
        url:             Fully-qualified URL.
        headers:         Auth + Accept headers.  Mutated in-place on token refresh so
                         all subsequent pages in the same paginator use the new token.
        params:          Query-string parameters.
        log:             ContextLogger from logging_utils.
        max_waits:       How many times to honour a Retry-After before giving up.
        token_refresher: Optional callable () -> dict of fresh auth headers.
                         When provided, a single 401 triggers one token refresh and
                         one immediate retry.  A second 401 raises HTTPError.

    Returns:
        requests.Response with a 2xx status code.

    Raises:
        RuntimeError:        if 429 persists beyond max_waits.
        requests.HTTPError:  for non-retriable 4xx / 5xx after urllib3 retries.
    """
    for attempt in range(1, max_waits + 1):
        resp = session.get(url, headers=headers, params=params, timeout=TIMEOUT)

        if resp.status_code == 429:
            wait = _parse_retry_after(resp)
            log.warning(
                f"Rate-limited  attempt={attempt}/{max_waits}"
                f"  sleeping={wait}s  url={url}"
            )
            if attempt < max_waits:
                time.sleep(wait)
            continue

        if resp.status_code == 401 and token_refresher is not None:
            log.warning(f"401 Unauthorized — refreshing token and retrying once  url={url}")
            headers.update(token_refresher())
            resp = session.get(url, headers=headers, params=params, timeout=TIMEOUT)
            # Second 401 after refresh → raise immediately, do not retry again
            resp.raise_for_status()
            return resp

        resp.raise_for_status()
        return resp

    raise RuntimeError(
        f"Rate limit unresolved after {max_waits} Retry-After waits  url={url}"
    )


def safe_post(
    session:         Session,
    url:             str,
    headers:         dict,
    params:          dict,
    body:            dict,
    log:             "ContextLogger",
    max_waits:       int = MAX_429_WAITS,
    token_refresher       = None,
) -> Response:
    """
    POST the URL with the same 429 / Retry-After and 401 refresh logic as safe_get.

    Intended for read-only POST endpoints (e.g. ADO WIQL) where retrying is safe.
    urllib3's automatic 5xx retry does NOT cover POST (not in allowed_methods) so
    this function provides equivalent manual retry for 429 and 401.

    Args:
        body:  JSON-serialisable dict sent as the request body.
        (other args: same meaning as safe_get)

    Returns:
        requests.Response with a 2xx status code.
    """
    for attempt in range(1, max_waits + 1):
        resp = session.post(url, headers=headers, params=params,
                            json=body, timeout=TIMEOUT)

        if resp.status_code == 429:
            wait = _parse_retry_after(resp)
            log.warning(
                f"Rate-limited (POST)  attempt={attempt}/{max_waits}"
                f"  sleeping={wait}s  url={url}"
            )
            if attempt < max_waits:
                time.sleep(wait)
            continue

        # urllib3 auto-retries 5xx for GET; POST must do it manually.
        # Honour Retry-After when present (some 503s include it), fall back to
        # exponential back-off — consistent with the 429 handling above.
        if resp.status_code in _RETRY_ON_STATUS:
            sleep = _parse_retry_after(
                resp, default=max(1, int(BACKOFF_FACTOR * (2 ** (attempt - 1))))
            )
            log.warning(
                f"Server error (POST)  attempt={attempt}/{max_waits}"
                f"  status={resp.status_code}  sleeping={sleep}s  url={url}"
            )
            if attempt < max_waits:
                time.sleep(sleep)
            continue

        if resp.status_code == 401 and token_refresher is not None:
            log.warning(f"401 Unauthorized (POST) — refreshing token and retrying once  url={url}")
            headers.update(token_refresher())
            resp = session.post(url, headers=headers, params=params,
                                json=body, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp

        resp.raise_for_status()
        return resp

    raise RuntimeError(
        f"Retries exhausted after {max_waits} attempts (POST)  url={url}"
    )
