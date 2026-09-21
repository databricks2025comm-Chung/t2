# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/paginators
# MAGIC Generator-based pagination handlers.  Each yields one page at a time as a
# MAGIC `list[dict]` so the caller never accumulates the full dataset in driver memory.
# MAGIC Parquet write is handled downstream in `landing_writer.write_pages_to_parquet()`.
# MAGIC
# MAGIC **Adding a new pagination style:**
# MAGIC 1. Write a generator matching the standard signature below.
# MAGIC 2. Register it in `PAGINATION_DISPATCH`.
# MAGIC 3. Set the matching string in `ingestion_entity_config.pagination_type`.
# MAGIC
# MAGIC Depends on: `logging_utils`, `http_client` (must be `%run` first).

# COMMAND ----------
import json as _json
import re   as _re
from typing import Iterator, Optional

_PAGE_SIZE       = 100   # records requested per API call — owns this value, not http_client
_WIQL_BATCH_SIZE = 200   # ADO /_apis/wit/workitems hard limit: max 200 IDs per GET
_ADO_API_VERSION = "7.1"

def _to_snow_dt(val: str) -> str:
    """
    Convert an ISO-8601 datetime to ServiceNow Table API format.
    ServiceNow sysparm_query expects 'YYYY-MM-DD HH:MM:SS' (space separator,
    no T, no Z, no microseconds). Safe to call on already-space-format input.
    """
    return val.replace("T", " ").split("+")[0].rstrip("Z").split(".")[0][:19]


# Maps filter_mode → (default_template, query_param_name, optional_value_transform)
# The value_transform callable is applied to wm_val before the template substitution.
# None means use wm_val as-is (correct for ISO-8601 APIs like OData/GitHub).
_FILTER_DEFAULTS: dict = {
    "odata":        ("{col} ge {val}",   "$filter",       None),
    "odata_quoted": ("{col} ge '{val}'", "$filter",       None),
    "sysparm":      ("{col}>={val}",     "sysparm_query", _to_snow_dt),
}

# ── Standard paginator signature ──────────────────────────────────────────────
# (session, base_url, endpoint, headers, wm_col, wm_val, log, token_refresher=None) -> Iterator[list[dict]]
#
# Each handler yields one page (list of record dicts) at a time.
# Buffering and Parquet write happen in landing_writer.write_pages_to_parquet().

# COMMAND ----------

def paginate_by_page_number(
    session:         "Session",
    base_url:        str,
    endpoint:        str,
    headers:         dict,
    wm_col:          str,
    wm_val:          str,
    log:             "ContextLogger",
    token_refresher  = None,
) -> Iterator[list[dict]]:
    """
    Increment ?page= from 1 until the API returns an empty collection.

    Typical sources: generic page-number APIs.

    NOTE — for GitHub, prefer paginate_by_link_header instead.  GitHub list
    endpoints return a top-level array and signal more pages via the Link
    response header; using the Link URL avoids a wasted last-page call and is
    more correct under concurrent writes (new records created mid-fetch don't
    shift page numbers and cause duplicates or gaps).

    The watermark column name is used directly as a query-string key, so the
    value in ingestion_entity_config.watermark_column_name must match the API's
    filter parameter name (e.g. 'since' for GitHub issues).

    Yields:
        list[dict] — one page of raw API records.
    """
    url  = f"{base_url}{endpoint}"
    page = 1

    while True:
        params = {
            "per_page": _PAGE_SIZE,
            "page":     page,
            wm_col:     wm_val,
        }
        resp  = safe_get(session, url, headers, params, log, token_refresher=token_refresher)
        body  = resp.json()
        # GitHub list endpoints return a top-level array.
        # GitHub search endpoints return {"items": [...]}.
        # OData-style fallback uses {"value": [...]}.
        if isinstance(body, list):
            batch = body
        elif isinstance(body, dict):
            if "items" in body:
                batch = body["items"]
            elif "value" in body:
                batch = body["value"]
            else:
                batch = []
        else:
            batch = []

        log.debug(f"PageNumber  page={page}  batch_size={len(batch)}")

        if not batch:
            return

        yield batch
        page += 1


def paginate_by_offset(
    session:         "Session",
    base_url:        str,
    endpoint:        str,
    headers:         dict,
    wm_col:          str,
    wm_val:          str,
    log:             "ContextLogger",
    token_refresher  = None,
) -> Iterator[list[dict]]:
    """
    Advance ?sysparm_offset= by _PAGE_SIZE until the API returns an empty result.

    Typical sources: ServiceNow Table API  (/table/incident, /table/problem …)

    NOTE — for compound ServiceNow queries (e.g. active=true^category=hardware
    combined with a watermark), use the Descriptor paginator instead.  Set
    filter_mode="sysparm" and provide a custom filter_template such as:
        "active=true^category=hardware^{col}>={val}"
    You can also add sysparm_fields, sysparm_display_value, and any other
    ServiceNow params via the extra_params key in the Descriptor config.

    Yields:
        list[dict] — one page of raw API records.
    """
    url    = f"{base_url}{endpoint}"
    offset = 0

    while True:
        params = {
            "sysparm_limit":  _PAGE_SIZE,
            "sysparm_offset": offset,
            "sysparm_query":  f"{wm_col}>={_to_snow_dt(wm_val)}",
        }
        resp  = safe_get(session, url, headers, params, log, token_refresher=token_refresher)
        batch = resp.json().get("result", [])

        log.debug(f"OffsetPagination  offset={offset}  batch_size={len(batch)}")

        if not batch:
            return

        yield batch
        offset += _PAGE_SIZE


def paginate_by_continuation_token(
    session:         "Session",
    base_url:        str,
    endpoint:        str,
    headers:         dict,
    wm_col:          str,
    wm_val:          str,
    log:             "ContextLogger",
    token_refresher  = None,
) -> Iterator[list[dict]]:
    """
    Follow the API's continuationToken until the response omits it.

    Typical sources: Azure DevOps REST API  (/_apis/wit/workitems …)

    Yields:
        list[dict] — one page of raw API records.
    """
    url              = f"{base_url}{endpoint}"
    token: Optional[str] = None
    page             = 0

    while True:
        params: dict = {
            "$top":        _PAGE_SIZE,
            "$filter":     f"{wm_col} ge '{wm_val}'",
            "api-version": _ADO_API_VERSION,
        }
        if token:
            params["continuationToken"] = token

        resp    = safe_get(session, url, headers, params, log, token_refresher=token_refresher)
        payload = resp.json()
        batch   = payload.get("value", [])
        # ADO sends the continuation token in the response HEADER, not the body.
        token   = resp.headers.get("x-ms-continuationtoken")
        page   += 1

        log.debug(
            f"ContinuationToken  page={page}"
            f"  batch_size={len(batch)}"
            f"  has_next={bool(token)}"
        )

        if not batch:
            return

        yield batch

        if not token:
            return


def _descriptor_params(
    desc:   dict,
    page:   int,
    offset: int,
    token:  Optional[str],
    wm_col: str,
    wm_val: str,
) -> dict:
    """Build the query-string params dict from a parsed pagination descriptor."""
    params: dict = {}

    page_size_param = desc.get("page_size_param")
    if page_size_param:
        params[page_size_param] = _PAGE_SIZE

    offset_mode  = desc.get("offset_mode", "none")
    offset_param = desc.get("offset_param")
    if offset_mode == "page" and offset_param:
        params[offset_param] = page
    elif offset_mode == "offset" and offset_param:
        params[offset_param] = offset

    if token:
        params["continuationToken"] = token

    filter_mode = desc.get("filter_mode", "querystring")
    if filter_mode == "querystring":
        params[wm_col] = wm_val
    elif filter_mode in _FILTER_DEFAULTS:
        default_tmpl, param_name, val_xform = _FILTER_DEFAULTS[filter_mode]
        tmpl = desc.get("filter_template") or default_tmpl
        val  = val_xform(wm_val) if val_xform else wm_val
        params[param_name] = tmpl.format(col=wm_col, val=val)
    else:
        raise ValueError(
            f"Unknown filter_mode='{filter_mode}' in Descriptor pagination_config. "
            f"Supported values: querystring, {', '.join(_FILTER_DEFAULTS)}. "
            "Check ingestion_entity_config.pagination_config."
        )

    order_by = desc.get("order_by")
    if order_by:
        params["$orderby"] = order_by.format(col=wm_col)

    # extra_params: arbitrary fixed params merged last so they can override anything above.
    # Examples: sysparm_fields, sysparm_display_value, $select, $expand, api-version.
    extra = desc.get("extra_params")
    if isinstance(extra, dict):
        params.update(extra)

    return params


def _descriptor_records(body, records_path: Optional[str]) -> list:
    """Extract the records list from a parsed response body."""
    if records_path is None:
        return body if isinstance(body, list) else []
    return body.get(records_path, []) if isinstance(body, dict) else []


def _parse_link_next(link_header: str) -> Optional[str]:
    """
    Parse the rel="next" URL from a GitHub-style Link response header.

    Example header value:
        <https://api.github.com/repos/octo/hello/issues?page=2>; rel="next",
        <https://api.github.com/repos/octo/hello/issues?page=5>; rel="last"

    Uses regex rather than comma-splitting so URLs that contain commas
    (e.g. cursor tokens or base64-encoded params) are handled correctly
    per RFC 8288, which allows commas inside the <URL> angle brackets.

    Returns the next-page URL string, or None when rel="next" is absent.
    """
    if not link_header:
        return None
    # [^>]+ captures the full URL (may contain commas); [^,]* limits the
    # match to one link-value, preventing rel="next" on entry N from
    # absorbing the URL of entry N+1.
    for m in _re.finditer(r'<([^>]+)>[^,]*?\brel="next"', link_header):
        return m.group(1)
    return None


def paginate_by_descriptor(
    session:          "Session",
    base_url:         str,
    endpoint:         str,
    headers:          dict,
    wm_col:           str,
    wm_val:           str,
    log:              "ContextLogger",
    token_refresher   = None,
    pagination_config: Optional[str] = None,
) -> Iterator[list[dict]]:
    """
    Config-driven generic paginator.  Handles standard REST pagination patterns
    without new Python code per API — only a JSON config row in the database.

    pagination_config is a JSON string stored in ingestion_entity_config.pagination_config.

    ── next_signal options ──────────────────────────────────────────────────────
    "empty_batch"           Stop when the API returns no records.
                            Requires offset_mode + offset_param to advance pages.
                            Sources: ServiceNow, any page/offset API.

    "link_header"           Follow rel="next" from the Link response header until
                            absent.  Page 2+ use the full header URL directly
                            (params={} so nothing extra is appended).
                            Sources: GitHub REST API (recommended over empty_batch).

    "odata_next_link"       Follow @odata.nextLink from the response body until
                            the field is absent.  Page 2+ use the full nextLink
                            URL directly (params={} so nothing extra is appended).
                            Sources: MS Graph, Dynamics 365, Dataverse, ADO Analytics,
                                     Power BI, SAP OData services.

    "continuation_header"   Read x-ms-continuationtoken from the response header
                            and pass it as continuationToken on the next request.
                            Sources: ADO REST (/_apis/wit/workitems …).

    ── filter_mode options ──────────────────────────────────────────────────────
    "querystring"    wm_col=wm_val as a direct query param.
                     filter_template is unused; wm_col IS the param name.
    "odata"          $filter={col} ge {val}  — no quotes around the value.
    "odata_quoted"   $filter={col} ge '{val}' — quoted string value.
    "sysparm"        sysparm_query={col}>={val}

    ── offset_mode options (for empty_batch signal) ─────────────────────────────
    "page"    Increment the page-number param (1 → 2 → 3 …).
    "offset"  Increment an offset param by _PAGE_SIZE (0 → 100 → 200 …).
    "none"    No offset param — only valid with continuation/nextLink signals.

    ── extra_params ─────────────────────────────────────────────────────────────
    Optional dict of fixed params merged into every request after all other params.
    Use for: sysparm_fields, sysparm_display_value, $select, $expand, api-version.
    These override anything the paginator builds automatically, so use with care.

    ── Example descriptors ──────────────────────────────────────────────────────
    GitHub Issues (recommended):
        {"page_size_param":"per_page","offset_mode":"none","records_path":null,
         "next_signal":"link_header","filter_mode":"querystring"}

    GitHub Search (records nested under "items"):
        {"page_size_param":"per_page","offset_mode":"none","records_path":"items",
         "next_signal":"link_header","filter_mode":"querystring",
         "extra_params":{"sort":"updated","order":"asc"}}

    ServiceNow (simple watermark only):
        {"page_size_param":"sysparm_limit","offset_param":"sysparm_offset",
         "offset_mode":"offset","records_path":"result","next_signal":"empty_batch",
         "filter_mode":"sysparm","filter_template":"{col}>={val}"}

    ServiceNow (compound query + field selection):
        {"page_size_param":"sysparm_limit","offset_param":"sysparm_offset",
         "offset_mode":"offset","records_path":"result","next_signal":"empty_batch",
         "filter_mode":"sysparm","filter_template":"active=true^{col}>={val}",
         "extra_params":{"sysparm_display_value":"false",
                         "sysparm_fields":"sys_id,number,short_description,state,sys_updated_on",
                         "sysparm_exclude_reference_link":"true"}}

    ADO Analytics / MS Graph / Dynamics (OData):
        {"page_size_param":"$top","offset_mode":"none","records_path":"value",
         "next_signal":"odata_next_link","filter_mode":"odata",
         "order_by":"{col} asc"}

    ADO REST (continuation token):
        {"page_size_param":"$top","offset_mode":"none","records_path":"value",
         "next_signal":"continuation_header","filter_mode":"odata_quoted",
         "extra_params":{"api-version":"7.1"}}

    Yields:
        list[dict] — one page of raw API records.
    """
    if not pagination_config:
        raise ValueError(
            "pagination_config must not be NULL for pagination_type='Descriptor'. "
            "Add a JSON descriptor to ingestion_entity_config.pagination_config."
        )

    desc   = _json.loads(pagination_config)
    signal = desc.get("next_signal", "empty_batch")

    if signal == "empty_batch" and desc.get("offset_mode", "none") == "none":
        raise ValueError(
            "Descriptor config error: next_signal='empty_batch' requires "
            "offset_mode='page' or offset_mode='offset' to advance between requests. "
            "With offset_mode='none', every request is identical and the generator "
            "loops forever. Use 'link_header' or 'odata_next_link' for cursor-based "
            "APIs, or set offset_mode='page'/'offset' for page/offset-number APIs."
        )

    url      = f"{base_url}{endpoint}"
    next_url: Optional[str] = None   # set when following nextLink (OData) or Link header
    page     = 1
    offset   = 0
    token: Optional[str] = None      # set when continuation_header provides one
    page_num = 0

    while True:
        if next_url:
            # Follow the pre-built next URL with no extra params (OData nextLink or Link header)
            actual_url, actual_params = next_url, {}
            next_url = None
        else:
            actual_url    = url
            actual_params = _descriptor_params(desc, page, offset, token, wm_col, wm_val)

        resp     = safe_get(session, actual_url, headers, actual_params, log,
                            token_refresher=token_refresher)
        body     = resp.json()
        batch    = _descriptor_records(body, desc.get("records_path"))
        page_num += 1

        log.debug(
            f"Descriptor  page={page_num}"
            f"  signal={signal}"
            f"  batch_size={len(batch)}"
        )

        if not batch:
            return

        yield batch

        # ── advance state ─────────────────────────────────────────────────────
        if signal == "empty_batch":
            offset_mode = desc.get("offset_mode", "none")
            if offset_mode == "page":
                page += 1
            elif offset_mode == "offset":
                offset += _PAGE_SIZE

        elif signal == "link_header":
            # GitHub REST API: Link: <url>; rel="next"
            next_url = _parse_link_next(resp.headers.get("Link", ""))
            if not next_url:
                return

        elif signal == "odata_next_link":
            next_url = body.get("@odata.nextLink") if isinstance(body, dict) else None
            if not next_url:
                return

        elif signal == "continuation_header":
            token = resp.headers.get("x-ms-continuationtoken")
            if not token:
                return

        else:
            log.warning(f"Descriptor: unknown next_signal='{signal}' — stopping")
            return


def paginate_by_odata(
    session:         "Session",
    base_url:        str,
    endpoint:        str,
    headers:         dict,
    wm_col:          str,
    wm_val:          str,
    log:             "ContextLogger",
    token_refresher  = None,
) -> Iterator[list[dict]]:
    """
    OData pagination — follows @odata.nextLink until absent.

    Typical sources: Azure DevOps Analytics API
        /_odata/v4.0/WorkItems
        /_odata/v4.0/WorkItemRevisions
        /_odata/v4.0/PipelineRuns
        /_odata/v4.0/TestRuns

    Page 1: builds URL + $filter/$top/$orderby params.
    Page 2+: follows @odata.nextLink directly — it is a complete URL with all
             parameters already encoded; no param reconstruction needed.

    IMPORTANT — ADO Analytics field names differ from ADO REST field names:
        REST API (System.ChangedDate)  →  Analytics (ChangedDate)
        REST API (System.WorkItemType) →  Analytics (WorkItemType)
    Set watermark_column_name to the Analytics name (no "System." prefix).

    ingestion_entity_config setup:
        pagination_type        = OData
        endpoint_url           = /_odata/v4.0/WorkItems
        watermark_column_name  = ChangedDate
        watermark_column_value = <ISO-8601 timestamp e.g. 2026-01-01T00:00:00Z>

    ingestion_source_config setup:
        base_url = https://analytics.dev.azure.com/{org}/{project}

    Yields:
        list[dict] — one page of raw OData records.
    """
    url    = f"{base_url}{endpoint}"
    page   = 0
    params = {
        "$top":     _PAGE_SIZE,
        "$filter":  f"{wm_col} ge {wm_val}",   # OData: no quotes around datetime value
        "$orderby": f"{wm_col} asc",            # stable ordering required for safe pagination
    }

    while True:
        resp     = safe_get(session, url, headers, params, log,
                            token_refresher=token_refresher)
        body     = resp.json()
        batch    = body.get("value", [])
        next_url = body.get("@odata.nextLink")
        page    += 1

        log.debug(
            f"OData  page={page}"
            f"  batch_size={len(batch)}"
            f"  has_next={bool(next_url)}"
        )

        if not batch:
            return

        yield batch

        if not next_url:
            return

        # Follow nextLink as-is — the full URL already carries $skiptoken and all
        # other params; passing params={} prevents requests from appending duplicates.
        url    = next_url
        params = {}


def paginate_by_wiql(
    session:          "Session",
    base_url:         str,
    endpoint:         str,
    headers:          dict,
    wm_col:           str,
    wm_val:           str,
    log:              "ContextLogger",
    token_refresher   = None,
    pagination_config: Optional[str] = None,
) -> Iterator[list[dict]]:
    """
    Azure DevOps WIQL two-step pagination — configurable work item type and fields.

    Step 1: POST /_apis/wit/wiql
        Sends a WIQL query built from pagination_config.wiql_where combined with
        the watermark filter.  Returns a flat list of work item IDs (up to
        $top=20,000).

    Step 2: GET /_apis/wit/workitems?ids=<batch>
        Fetches field data for up to 200 IDs per request (ADO hard limit).
        Use pagination_config.fields to fetch only the columns you need —
        narrower payloads are significantly faster than $expand=all.

    ── pagination_config keys ────────────────────────────────────────────────
    wiql_where  STRING  Body of the WIQL WHERE clause.  The watermark condition
                        ( AND [{wm_col}] >= '{wm_val}' ) is appended automatically;
                        do not include it here.  Wrap field names in [ ].
                        Default: "[System.WorkItemType] = 'Epic'"

    fields      LIST    ADO field reference names to retrieve in Step 2.
                        When omitted, falls back to $expand=all (convenient but
                        slow for wide work item types — avoid in production).

    ── Example configs ──────────────────────────────────────────────────────
    Epics only (backward-compatible — same as pagination_config = NULL):
        {"wiql_where": "[System.WorkItemType] = 'Epic'"}

    Features with an explicit field list (recommended for production):
        {"wiql_where": "[System.WorkItemType] = 'Feature'",
         "fields": ["System.Id","System.Title","System.WorkItemType",
                    "System.State","System.AreaPath","System.ChangedDate",
                    "Microsoft.VSTS.Common.Priority"]}

    Bugs and User Stories in a specific area path:
        {"wiql_where": "[System.WorkItemType] IN ('Bug','User Story') AND [System.AreaPath] UNDER 'MyProject\\\\Platform'",
         "fields": ["System.Id","System.Title","System.WorkItemType",
                    "System.State","System.AssignedTo","System.ChangedDate"]}

    ── ingestion_entity_config setup ────────────────────────────────────────
        pagination_type        = WIQL
        endpoint_url           = /_apis/wit/workitems
        watermark_column_name  = System.ChangedDate
        watermark_column_value = <ISO-8601 timestamp e.g. 2020-01-01T00:00:00Z>
        pagination_config      = {"wiql_where": "..."}   (omit to default to Epics)

    Yields:
        list[dict] — one batch of work item records.
    """
    cfg_dict: dict        = _json.loads(pagination_config) if pagination_config else {}
    wiql_where: str       = cfg_dict.get("wiql_where", "[System.WorkItemType] = 'Epic'")
    fields: Optional[list] = cfg_dict.get("fields")

    # ── Step 1: POST WIQL query → get all matching IDs ───────────────────
    wiql_url  = f"{base_url}/_apis/wit/wiql"
    # WIQL datetime literals must be in 'YYYY-MM-DD HH:MM:SS' format (space separator,
    # no T, no Z).  ISO-8601 with T/Z is valid in OData $filter and ADO REST $filter
    # but is not part of the WIQL spec and causes parse errors on some ADO tenants.
    wiql_body = {
        "query": (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE {wiql_where} "
            f"AND [{wm_col}] >= '{_to_snow_dt(wm_val)}' "
            f"ORDER BY [{wm_col}] ASC"
        )
    }
    wiql_params = {"api-version": _ADO_API_VERSION, "$top": 20_000}

    resp    = safe_post(session, wiql_url, headers, wiql_params, wiql_body, log,
                        token_refresher=token_refresher)
    all_ids = [item["id"] for item in resp.json().get("workItems", [])]

    log.info(
        f"WIQL  where={wiql_where!r}"
        f"  {wm_col}>={wm_val}"
        f"  ids_returned={len(all_ids)}"
    )

    if not all_ids:
        return

    # ── Step 2: GET field data in batches of 200 (ADO hard limit) ────────
    details_url   = f"{base_url}{endpoint}"
    total_batches = (len(all_ids) + _WIQL_BATCH_SIZE - 1) // _WIQL_BATCH_SIZE

    for batch_num, start in enumerate(range(0, len(all_ids), _WIQL_BATCH_SIZE), 1):
        batch_ids = all_ids[start : start + _WIQL_BATCH_SIZE]
        params: dict = {
            "ids":         ",".join(str(i) for i in batch_ids),
            "api-version": _ADO_API_VERSION,
        }
        if fields:
            # Explicit field list: narrower payload, faster, recommended for production.
            params["fields"] = ",".join(fields)
        else:
            # Fallback: fetch all fields.  Convenient but expensive for wide types.
            params["$expand"] = "all"

        resp  = safe_get(session, details_url, headers, params, log,
                         token_refresher=token_refresher)
        batch = resp.json().get("value", [])

        log.debug(
            f"WIQL  batch={batch_num}/{total_batches}"
            f"  ids={len(batch_ids)}  records={len(batch)}"
        )

        if batch:
            yield batch


def paginate_by_link_header(
    session:         "Session",
    base_url:        str,
    endpoint:        str,
    headers:         dict,
    wm_col:          str,
    wm_val:          str,
    log:             "ContextLogger",
    token_refresher  = None,
) -> Iterator[list[dict]]:
    """
    Follow the rel="next" URL in the Link response header until absent.

    Recommended for: GitHub REST API (all list and search endpoints).

    Why Link-header pagination is preferred over PageNumber for GitHub:
    ───────────────────────────────────────────────────────────────────
    1.  No wasted last-page call — stops as soon as rel="next" is absent,
        saving one HTTP round-trip per run.
    2.  Correct under concurrent writes — GitHub's Link URL encodes a cursor,
        not a raw page number.  Records created mid-fetch don't shift page
        boundaries and cause duplicated or skipped rows.
    3.  GitHub's own documented recommendation for API consumers.

    Page 1: builds URL with per_page + watermark filter.
    Page 2+: follows the Link header's rel="next" URL exactly (params={} so
             requests does not append duplicate params).

    Response body shapes supported:
        Top-level list     [...]          GitHub list endpoints (issues, pulls, commits …)
        {"items": [...]}                  GitHub search endpoints (/search/issues …)
        {"value": [...]}                  OData-style fallback

    watermark_column_name must match GitHub's filter parameter name:
        issues / pull requests  → "since"   (filters by updated_at)
        commits                 → "since"   (filters by committer date)
        repos / members         → no filter (omit by setting wm_col to a no-op param
                                             or accept a full load each run)

    ingestion_entity_config setup:
        pagination_type        = LinkHeader
        endpoint_url           = /repos/{owner}/{repo}/issues
        watermark_column_name  = since
        watermark_column_value = <ISO-8601 timestamp e.g. 2026-01-01T00:00:00Z>

    Yields:
        list[dict] — one page of raw API records.
    """
    url    = f"{base_url}{endpoint}"
    page   = 0
    params = {
        "per_page": _PAGE_SIZE,
        wm_col:     wm_val,
    }

    while True:
        resp = safe_get(session, url, headers, params, log,
                        token_refresher=token_refresher)
        body = resp.json()

        if isinstance(body, list):
            batch = body
        elif isinstance(body, dict):
            # GitHub search: {"total_count": N, "items": [...]}
            # OData fallback: {"value": [...]}
            if "items" in body:
                batch = body["items"]
            elif "value" in body:
                batch = body["value"]
            else:
                batch = []
        else:
            batch = []

        next_url = _parse_link_next(resp.headers.get("Link", ""))
        page    += 1

        log.debug(
            f"LinkHeader  page={page}"
            f"  batch_size={len(batch)}"
            f"  has_next={bool(next_url)}"
        )

        if not batch:
            return

        yield batch

        if not next_url:
            return

        # Follow the Link header URL exactly — it already carries per_page, cursor,
        # and all other params; passing params={} prevents requests appending duplicates.
        url    = next_url
        params = {}


# ── Dispatch map ──────────────────────────────────────────────────────────────
PAGINATION_DISPATCH: dict = {
    "PageNumber":        paginate_by_page_number,
    "OffsetPagination":  paginate_by_offset,
    "ContinuationToken": paginate_by_continuation_token,
    "OData":             paginate_by_odata,
    "WIQL":              paginate_by_wiql,
    "LinkHeader":        paginate_by_link_header,
    "Descriptor":        paginate_by_descriptor,
}
