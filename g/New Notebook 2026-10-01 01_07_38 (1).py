# Databricks notebook source
# DBTITLE 1,audit
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
    run_id:  str,
    payload: dict,
    log:     "ContextLogger",
) -> None:
    """
    MERGE one audit row into ingestion_audit_log, keyed by run_id.

    Using MERGE (not INSERT) makes every close idempotent — re-running a
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
              ON  tgt.run_id = src.run_id
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT  *
        """)
    finally:
        # Always drop — keeps the session catalogue clean regardless of outcome.
        spark.catalog.dropTempView(view_name)

    log.debug(f"Audit upserted  status={payload.get('status')}")


def build_audit_payload(
    run_id:    str,
    cfg,
    nb_name:   str,
    run_start: datetime,
    status:    str,
    remarks:   str,
    run_end:        Optional[datetime] = None,
    records_read:   Optional[int]     = None,
    records_loaded: Optional[int]     = None,
    records_failed: Optional[int]     = None,
    file_path: str              = "",
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
        "run_id":                  run_id,
        "source_id":               cfg.source_id,
        "entity_id":               cfg.entity_id,
        "run_start_time":          run_start,
        "run_end_time":            run_end,
        "status":                  status,
        "records_read":            records_read,
        "records_loaded":          records_loaded,
        "records_failed":          records_failed,
        "execution_duration_sec":  duration,
        "notebook_name":           nb_name,
        "remarks":                 remarks[:2000] if remarks else remarks,
        "file_path":               file_path,
    }


# COMMAND ----------

# DBTITLE 1,auth
# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/auth
# MAGIC Builds HTTP auth headers from Databricks Secrets.
# MAGIC Add a new `elif` block here when onboarding a source with a different scheme.
# MAGIC Depends on: `logging_utils` (must be `%run` first).
# MAGIC
# MAGIC Supported auth_type values:
# MAGIC   PAT          Azure DevOps PAT — Basic auth base64(:<token>)
# MAGIC   ADO_OAUTH    Azure DevOps via Azure AD / Entra ID client credentials — recommended for new ADO onboarding
# MAGIC   GITHUB_PAT   GitHub PAT (classic or fine-grained) — Bearer <token>
# MAGIC   GITHUB_APP   GitHub App installation token — short-lived, auto-refreshed on 401
# MAGIC   SNOW_OAUTH   ServiceNow OAuth2 client credentials — Bearer token, auto-refreshed on 401
# MAGIC   SNOW_BASIC   ServiceNow Basic auth — username:password (legacy only, not recommended)
# MAGIC   OAUTH2       Generic OAuth2 bearer (static token in secret)
# MAGIC   TOKEN        Generic API key bearer (SonarQube, Checkmarx, etc.)

# COMMAND ----------
import base64
import json      as _json
import time      as _time
import requests  as _requests
from datetime import datetime  as _datetime, timezone as _timezone
from cryptography.hazmat.primitives            import hashes         as _hashes
from cryptography.hazmat.primitives            import serialization  as _serialization
from cryptography.hazmat.primitives.asymmetric import padding        as _padding
import os as _os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM

SECRET_SCOPE = "ingestion-secrets"   # Databricks secret scope name

# GitHub API version header — pin this so behaviour doesn't silently change
_GITHUB_API_VERSION = "2022-11-28"

# (connect_timeout_s, read_timeout_s) for all token-endpoint POST requests.
# 5 s connect covers slow DNS / TLS handshake; 30 s read covers Azure AD / SNOW
# token endpoints under load.  Databricks Secrets REST API uses a tighter 10 s.
_TOKEN_REQ_TIMEOUT:   tuple = (5, 30)
_SECRETS_API_TIMEOUT: int   = 10

# Side-channel: active OAuth helpers write expires_in here after each L3 fetch.
# Only consumed by the legacy prefetch_token() function (unused in production).
# Key = key_prefix, value = expires_in seconds from the token endpoint response.
_FRESH_EXPIRES_IN: dict = {}

# Legacy guard used by the old prefetch_token() / _read_prefetched_token() path.
# No longer has any effect — _read_prefetched_token() is not called in production.
_PREFETCH_IN_PROGRESS: set = set()

# Key suffix for orchestrator-prefetched tokens stored in Databricks Secrets:
#   {SECRET_SCOPE} / {key_prefix}{_PREFETCH_KEY_SUFFIX}  →  {"token": "...", "expires_at": 1234567890.0}
_PREFETCH_KEY_SUFFIX: str = "-prefetched-token"

# Databricks Secrets key name for the AES-256 token-cache encryption key.
# Generate once with _auth_session/setup_encryption_key.py and store in Databricks Secrets.
# Rotate manually — no code change needed.
_TOKEN_CACHE_KEY_NAME: str = "token-cache-aes-key"

# COMMAND ----------
# ── LEGACY — Databricks-Secrets-based token prefetch (unused) ─────────────────
# _read_prefetched_token, _write_prefetched_token, and prefetch_token implement
# the original L2 token-cache approach that wrote short-lived OAuth tokens
# directly to Databricks Secrets between the orchestrator and entity extractors.
# Replaced by AES-256-GCM encrypt_token / decrypt_token and the shared Delta
# table catalog.ingestion.token_cache — orchestrator writes the ciphertext once;
# entity extractors read from it at startup.
# Kept for reference / rollback only.  Not called from any production code.
# ──────────────────────────────────────────────────────────────────────────────

def _read_prefetched_token(key_prefix: str) -> "str | None":
    """
    Read the orchestrator-prefetched token from Databricks Secrets.
    Returns the token string if present and not expired, None otherwise.
    Returns None immediately when the orchestrator is mid-prefetch for this
    key_prefix (bypasses L2 so prefetch_token() always calls L3 fresh).
    Silent on all other misses so standalone runs fall through to L3.
    """
    if key_prefix in _PREFETCH_IN_PROGRESS:
        return None
    try:
        raw  = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}{_PREFETCH_KEY_SUFFIX}")
        data = _json.loads(raw)
        if _time.time() < data.get("expires_at", 0):
            return data["token"]
    except Exception:
        pass
    return None


def _write_prefetched_token(
    key_prefix: str,
    token:      str,
    expires_at: float,
    log:        "ContextLogger",
) -> None:
    """
    Write a prefetched token to Databricks Secrets via the Databricks REST API.
    Called once by 00_Orchestrator_Ingestion before the For Each task launches.
    Requires WRITE on the ingestion-secrets scope for the cluster's service principal.
    Non-fatal: write failures log a warning so entity extractors fall back to L3.
    """
    try:
        # spark.conf.get works on all cluster types (job, interactive, single-node).
        # browserHostName() is interactive-only and returns empty on many job clusters.
        _host = spark.conf.get("spark.databricks.workspaceUrl", "")
        _tkn  = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        if not _host:
            raise RuntimeError(
                "spark.databricks.workspaceUrl is empty — cannot resolve Secrets API endpoint"
            )
        if not _tkn:
            raise RuntimeError(
                "cluster API token is empty — check cluster token generation settings"
            )
        for _attempt in range(1, 3):   # one retry on transient 5xx
            _resp = _requests.post(
                f"https://{_host}/api/2.0/secrets/put",
                headers={"Authorization": f"Bearer {_tkn}"},
                json={
                    "scope":        SECRET_SCOPE,
                    "key":          f"{key_prefix}{_PREFETCH_KEY_SUFFIX}",
                    "string_value": _json.dumps({"token": token, "expires_at": expires_at}),
                },
                timeout=_SECRETS_API_TIMEOUT,
            )
            if _resp.status_code < 500 or _attempt == 2:
                break
            _time.sleep(2)
        _resp.raise_for_status()
        log.info(f"Token stored in Databricks Secrets  key={key_prefix}{_PREFETCH_KEY_SUFFIX}")
    except Exception as _e:
        _status = getattr(getattr(_e, "response", None), "status_code", None)
        _hint = (
            f"  Ensure the cluster service principal has WRITE on scope='{SECRET_SCOPE}'."
            if _status in (401, 403) else ""
        )
        log.warning(
            f"Token prefetch write failed — entity extractors will fetch their own tokens.  "
            f"error={_e}{_hint}"
        )


# COMMAND ----------

def _b64url(data: bytes) -> str:
    """URL-safe base64 without padding (used for JWT segments)."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _make_github_jwt(app_id: str, private_key_pem: str) -> str:
    """
    Build a GitHub App JWT signed with RS256 using only the `cryptography`
    package (bundled with every Databricks Runtime — no extra installs needed).

    Issued-at is backdated 60 seconds to absorb clock skew between the Spark
    driver and GitHub's servers.  Expiry is set at 9 minutes (GitHub's hard
    limit is 10 minutes, so this leaves a 1-minute safety margin).
    """
    now = int(_time.time())
    header  = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iat": now - 60,    # 60-second clock-skew buffer
        "exp": now + 540,   # 9 minutes (< 10-minute GitHub limit)
        "iss": str(app_id),
    }
    header_enc  = _b64url(_json.dumps(header,  separators=(",", ":")).encode())
    payload_enc = _b64url(_json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header_enc}.{payload_enc}".encode()

    pem_bytes = (
        private_key_pem.replace("\r\n", "\n").replace("\r", "\n").encode()
        if isinstance(private_key_pem, str) else private_key_pem
    )
    private_key = _serialization.load_pem_private_key(pem_bytes, password=None)
    signature = private_key.sign(signing_input, _padding.PKCS1v15(), _hashes.SHA256())
    return f"{header_enc}.{payload_enc}.{_b64url(signature)}"


def _get_github_app_headers(key_prefix: str, log: "ContextLogger") -> dict[str, str]:
    """
    Exchange a GitHub App private key for a short-lived installation access token
    (valid for 1 hour), then return the full set of headers for all GitHub API calls.

    Secrets required in Databricks secret scope (SECRET_SCOPE):
        <prefix>-app-id             GitHub App numeric ID (stored as plain string)
        <prefix>-installation-id    Installation numeric ID (stored as plain string)
        <prefix>-private-key        Full PEM contents of the app's RSA private key

    How rotation and refresh work
    ──────────────────────────────
    Installation tokens expire after exactly 1 hour.  On any 401, the HTTP helpers
    call token_refresher() (defined in 01_Entity_Extractor).  It first checks
    catalog.ingestion.token_cache (L2); if a peer entity already refreshed, it uses
    that token.  Otherwise it calls get_auth_headers() here for a fresh L3 token,
    writes the result back to the cache, and continues — all transparently mid-run.

    Private key rotation: store the new PEM in the Databricks secret.  The next
    token refresh (on the next 401, or the next run) picks it up automatically.
    """
    app_id          = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-app-id")
    installation_id = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-installation-id")
    private_key_pem = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-private-key")

    jwt_token = _make_github_jwt(app_id, private_key_pem)

    for _attempt in range(1, 3):   # one retry on transient 5xx
        resp = _requests.post(
            f"https://api.github.com/app/installations/{installation_id}/access_tokens",
            headers={
                "Authorization":        f"Bearer {jwt_token}",
                "Accept":               "application/vnd.github+json",
                "X-GitHub-Api-Version": _GITHUB_API_VERSION,
            },
            timeout=_TOKEN_REQ_TIMEOUT,
        )
        if resp.status_code < 500 or _attempt == 2:
            break
        _time.sleep(2)
    if not resp.ok:
        raise RuntimeError(
            f"GitHub App installation token request failed"
            f"  status={resp.status_code}"
            f"  installation_id={installation_id}"
            f"  body={resp.text[:300]}"
        )
    body = resp.json()
    installation_token = body.get("token")
    if not installation_token:
        raise RuntimeError(
            f"GitHub App token response missing 'token' field"
            f"  installation_id={installation_id}"
            f"  Response keys: {list(body)}"
        )

    # GitHub returns expires_at (ISO timestamp), not expires_in (seconds).
    # Parse it to compute actual remaining lifetime for the side-channel.
    try:
        _exp_dt    = _datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00"))
        expires_in = max(0, int((_exp_dt - _datetime.now(_timezone.utc)).total_seconds()))
    except Exception:
        expires_in = 3600
    _FRESH_EXPIRES_IN[key_prefix] = expires_in

    log.debug(
        f"GitHub App token acquired"
        f"  app_id={app_id}"
        f"  installation_id={installation_id}"
        f"  expires_in={expires_in}s"
    )
    return {
        "Authorization":        f"Bearer {installation_token}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": _GITHUB_API_VERSION,
    }


# COMMAND ----------

def _get_snow_oauth_headers(
    key_prefix: str,
    base_url:   str,
    log:        "ContextLogger",
) -> dict[str, str]:
    """
    Exchange ServiceNow OAuth2 client credentials for a short-lived bearer token.

    ServiceNow OAuth2 token endpoint: POST {base_url}/oauth_token.do
    Grant type: client_credentials (server-to-server, no user interaction).
    Token lifetime: 1800 seconds (30 minutes) by default; configurable per instance
    under System OAuth → Application Registry → token lifespan.

    Secrets required in Databricks secret scope (SECRET_SCOPE):
        <prefix>-client-id      OAuth2 client ID from the ServiceNow Application Registry
        <prefix>-client-secret  OAuth2 client secret from the Application Registry

    How to create the Application Registry in ServiceNow:
        1.  Navigate to System OAuth → Application Registry → New
        2.  Choose "Create an OAuth API endpoint for external clients"
        3.  Set the grant type to "Client Credentials" (no refresh token needed)
        4.  Copy the client_id and generate a client_secret
        5.  Store both in Databricks Secrets under the keys above

    Refresh behaviour
    ──────────────────
    Tokens expire after ~30 minutes.  On any 401, the HTTP helpers call
    token_refresher() (defined in 01_Entity_Extractor).  It first checks
    catalog.ingestion.token_cache (L2); if a peer entity already refreshed, it
    uses that token.  Otherwise it calls get_auth_headers() here for a fresh L3
    token, writes the result back to the cache, and continues — all transparently,
    mid-run, without restarting the notebook.

    Secret rotation: update the client_secret in Databricks Secrets.  The next
    token refresh picks it up automatically; no notebook changes required.
    """
    client_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-client-id")
    client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-client-secret")

    token_url = f"{base_url.rstrip('/')}/oauth_token.do"

    for _attempt in range(1, 3):   # one retry on transient 5xx
        resp = _requests.post(
            token_url,
            # ServiceNow requires application/x-www-form-urlencoded, not JSON
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
            },
            timeout=_TOKEN_REQ_TIMEOUT,
        )
        if resp.status_code < 500 or _attempt == 2:
            break
        _time.sleep(2)
    if not resp.ok:
        raise RuntimeError(
            f"ServiceNow OAuth2 token request failed"
            f"  status={resp.status_code}"
            f"  url={token_url}"
            f"  body={resp.text[:300]}"
        )

    body  = resp.json()
    token = body.get("access_token")
    if not token:
        raise RuntimeError(
            f"ServiceNow OAuth2 response missing 'access_token'  url={token_url}  "
            f"Verify client_id / client_secret in scope='{SECRET_SCOPE}'.  "
            f"Response keys: {list(body)}"
        )

    expires_in = int(body.get("expires_in", 1800))
    _FRESH_EXPIRES_IN[key_prefix] = expires_in
    log.debug(
        f"ServiceNow OAuth2 token acquired"
        f"  instance={base_url}"
        f"  expires_in={expires_in}s"
    )
    return {"Authorization": f"Bearer {token}"}


# COMMAND ----------

def _get_ado_oauth_headers(key_prefix: str, log: "ContextLogger") -> dict[str, str]:
    """
    Acquire an Azure AD / Entra ID access token for Azure DevOps using
    the OAuth2 client credentials flow.

    Token endpoint:  https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
    Grant type:      client_credentials
    Scope:           https://app.vssps.visualstudio.com/.default
    Token lifetime:  3600 seconds (1 hour) — refreshed transparently on 401 via token_refresher.

    Secrets required in Databricks secret scope (SECRET_SCOPE):
        <prefix>-tenant-id      Azure AD directory (tenant) ID
        <prefix>-client-id      App registration client ID
        <prefix>-client-secret  App registration client secret

    Setup in Azure / ADO:
        1.  Register an application in Microsoft Entra ID (Azure AD).
        2.  Under "Certificates & secrets", create a client secret and store it.
        3.  In Azure DevOps Organisation Settings → Users, add the service principal
            as a member and assign it the required access level (Basic or above).
        4.  Grant the specific ADO permissions the pipeline needs — e.g.
            vso.work_write for work items, vso.build_read for pipeline data.
        5.  Store tenant_id, client_id, and client_secret in Databricks Secrets.

    Why client_credentials over PAT
    ─────────────────────────────────
    PATs are tied to a human account and expire; rotating them requires a person.
    Service principals authenticate independently of any employee, survive
    off-boarding, and have their credentials managed programmatically.

    Token rotation: update the client_secret in Databricks Secrets.  The next
    token refresh (on the next 401, or the next run) picks it up automatically;
    no notebook changes required.
    """
    tenant_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-tenant-id")
    client_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-client-id")
    client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-client-secret")

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    for _attempt in range(1, 3):   # one retry on transient 5xx
        resp = _requests.post(
            token_url,
            # Must be application/x-www-form-urlencoded (not JSON)
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         "https://app.vssps.visualstudio.com/.default",
            },
            timeout=_TOKEN_REQ_TIMEOUT,
        )
        if resp.status_code < 500 or _attempt == 2:
            break
        _time.sleep(2)
    if not resp.ok:
        raise RuntimeError(
            f"ADO OAuth2 token request failed"
            f"  status={resp.status_code}"
            f"  tenant_id={tenant_id}"
            f"  url={token_url}"
            f"  body={resp.text[:300]}"
        )

    body  = resp.json()
    token = body.get("access_token")
    if not token:
        raise RuntimeError(
            f"ADO OAuth2 response missing 'access_token'"
            f"  tenant_id={tenant_id}"
            f"  Verify client_id / client_secret in scope='{SECRET_SCOPE}'."
            f"  Response keys: {list(body)}"
        )

    expires_in = int(body.get("expires_in", 3600))
    _FRESH_EXPIRES_IN[key_prefix] = expires_in
    log.debug(
        f"ADO OAuth2 token acquired"
        f"  tenant_id={tenant_id}"
        f"  expires_in={expires_in}s"
    )
    return {"Authorization": f"Bearer {token}"}


def get_auth_headers(
    auth_type:   str,
    source_name: str,
    log:         "ContextLogger",
    base_url:    str = "",
) -> dict[str, str]:
    """
    Return HTTP Authorization (and source-specific) headers for the given auth scheme.

    Reads credentials from Databricks Secrets — nothing is logged or hardcoded.

    Secret key convention  (key_prefix = source_name.lower().replace(" ", "-")):
        <prefix>-pat               PAT / GitHub PAT    (PAT, GITHUB_PAT)
        <prefix>-oauth-token       OAuth2 bearer       (OAUTH2, static)
        <prefix>-api-key           Generic API key     (TOKEN)
        <prefix>-app-id            GitHub App ID       (GITHUB_APP)
        <prefix>-installation-id   Installation ID     (GITHUB_APP)
        <prefix>-private-key       RSA PEM key         (GITHUB_APP)
        <prefix>-tenant-id         Azure AD tenant ID  (ADO_OAUTH)
        <prefix>-client-id         OAuth2 client ID    (ADO_OAUTH, SNOW_OAUTH)
        <prefix>-client-secret     OAuth2 secret       (ADO_OAUTH, SNOW_OAUTH)
        <prefix>-username          Service account     (SNOW_BASIC)
        <prefix>-password          Service account pw  (SNOW_BASIC)

    source_name must contain only letters, digits, and spaces.

    Returns a dict of headers merged into the request.  Source-specific
    headers such as Accept and X-GitHub-Api-Version are included here so the
    extractor does not need per-source logic.

    The returned dict is injected into (and updated in-place in) the shared
    headers dict that each paginator reuses across pages.  When token_refresher
    is called on a 401, headers.update(token_refresher()) replaces only the
    keys returned here, leaving caller-added headers (e.g. Content-Type) intact.

    Args:
        auth_type:   Value from ingestion_source_config.auth_type.
        source_name: Value from ingestion_source_config.source_name.
        log:         ContextLogger from logging_utils.
        base_url:    Value from ingestion_source_config.base_url.
                     Required for SNOW_OAUTH (token endpoint is {base_url}/oauth_token.do).
                     Ignored by all other auth types.

    Raises:
        ValueError: for an unrecognised auth_type.
    """
    key_prefix      = source_name.lower().replace(" ", "-")
    auth_type_upper = auth_type.upper()

    # Fail fast — invalid chars produce a silently wrong key_prefix which then
    # raises a cryptic "secret not found" error from Databricks rather than
    # pointing at the real cause.  Allowed: letters, digits, spaces (→ hyphens),
    # hyphens, underscores.  All map to valid Databricks secret key characters.
    if not all(c.isalnum() or c in " -_" for c in source_name):
        raise ValueError(
            f"source_name='{source_name}' contains characters beyond letters, digits, "
            "spaces, hyphens and underscores. These produce an invalid Databricks secret "
            "key prefix. Fix the value in ingestion_source_config.source_name."
        )

    log.debug(f"Building auth headers  auth_type={auth_type}  key_prefix={key_prefix}")

    if auth_type_upper == "PAT":
        # Azure DevOps PAT — Basic auth with base64(:<token>)
        token   = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-pat")
        encoded = base64.b64encode(f":{token}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    elif auth_type_upper == "GITHUB_PAT":
        # GitHub PAT (classic or fine-grained).
        # PATs don't expire by default; token_refresher() re-reads the secret on
        # any 401, so a manually rotated PAT is picked up automatically.
        token = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-pat")
        return {
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": _GITHUB_API_VERSION,
        }

    elif auth_type_upper == "GITHUB_APP":
        # GitHub App installation token — expires after 1 hour.
        # See _get_github_app_headers() for the full refresh story.
        return _get_github_app_headers(key_prefix, log)

    elif auth_type_upper == "SNOW_OAUTH":
        # ServiceNow OAuth2 client credentials — recommended for production.
        # Tokens expire in ~30 minutes; token_refresher() re-acquires on any 401.
        # Requires base_url so the token endpoint ({base_url}/oauth_token.do) can
        # be constructed — passed from cfg.base_url via the token_refresher lambda.
        if not base_url:
            raise ValueError(
                "SNOW_OAUTH requires base_url (ingestion_source_config.base_url). "
                "Ensure the token_refresher lambda passes cfg.base_url."
            )
        return _get_snow_oauth_headers(key_prefix, base_url, log)

    elif auth_type_upper == "SNOW_BASIC":
        # ServiceNow Basic auth — username:password.
        # Use only for legacy instances that do not support OAuth2.
        # Prefer SNOW_OAUTH for all new onboarding.
        username = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-username")
        password = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-password")
        encoded  = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    elif auth_type_upper == "ADO_OAUTH":
        # Azure DevOps OAuth2 via Azure AD / Entra ID client credentials.
        # Recommended over PAT for new onboarding — not tied to a human account.
        # Tokens expire after 1 hour; token_refresher() re-acquires on any 401.
        # See _get_ado_oauth_headers() for setup instructions and the refresh story.
        return _get_ado_oauth_headers(key_prefix, log)

    elif auth_type_upper == "OAUTH2":
        # Generic OAuth2 bearer — static token stored in secret.
        # For sources requiring client-credentials refresh, replace this static
        # read with a dedicated helper (see SNOW_OAUTH as a reference pattern).
        token = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-oauth-token")
        return {"Authorization": f"Bearer {token}"}

    elif auth_type_upper == "TOKEN":
        # Generic API token auth — used by SonarQube, Checkmarx, and similar tools.
        key = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-api-key")
        return {"Authorization": f"Bearer {key}"}

    else:
        raise ValueError(
            f"auth_type='{auth_type}' is not supported. "
            "Supported values: PAT, ADO_OAUTH, GITHUB_PAT, GITHUB_APP, SNOW_OAUTH, SNOW_BASIC, OAUTH2, TOKEN. "
            "Add an elif block in _lib/auth.py → get_auth_headers() for new schemes."
        )


# COMMAND ----------

def prefetch_token(
    auth_type:   str,
    source_name: str,
    base_url:    str,
    log:         "ContextLogger",
) -> None:
    """
    LEGACY (unused): originally called by 00_Orchestrator_Ingestion to fetch the
    OAuth token once and store it in Databricks Secrets for all entity extractors.

    Replaced by the current approach in the orchestrator: get_auth_headers() +
    encrypt_token() + write_token_cache(), which writes an AES-256-GCM ciphertext
    to catalog.ingestion.token_cache (Delta table) instead of Databricks Secrets.
    Kept for reference only.  Not called from any production code.
    """
    auth_type_upper = auth_type.upper()
    if auth_type_upper not in ("ADO_OAUTH", "SNOW_OAUTH", "GITHUB_APP"):
        log.debug(f"Token prefetch skipped — not required for auth_type={auth_type}")
        return

    key_prefix = source_name.lower().replace(" ", "-")

    # Force L3 by marking this key_prefix as in-progress before calling
    # get_auth_headers().  _read_prefetched_token() returns None for any
    # key in _PREFETCH_IN_PROGRESS, so L2 is bypassed even on re-runs where
    # a valid token is already stored in Databricks Secrets.
    _PREFETCH_IN_PROGRESS.add(key_prefix)
    try:
        headers = get_auth_headers(auth_type, source_name, log, base_url)
    finally:
        _PREFETCH_IN_PROGRESS.discard(key_prefix)
    token = headers["Authorization"].split(" ", 1)[1]

    # Use the actual lifetime from the token endpoint response (written to
    # _FRESH_EXPIRES_IN by the L3 fetch above).  ADO / SNOW supply expires_in
    # (seconds); GitHub App supplies expires_at (ISO timestamp) which is parsed
    # to seconds in _get_github_app_headers.  Fall back to known defaults only
    # when the response is missing the field or the parse fails.
    _default = {"ADO_OAUTH": 3600, "SNOW_OAUTH": 1800, "GITHUB_APP": 3600}
    expires_in = _FRESH_EXPIRES_IN.pop(key_prefix, _default[auth_type_upper])
    expires_at = _time.time() + expires_in - 300   # 5-minute safety margin

    _write_prefetched_token(key_prefix, token, expires_at, log)


# COMMAND ----------
# ── AES-256-GCM token cache ────────────────────────────────────────────────────
#
# Why AES-256-GCM instead of Fernet (cryptography.fernet.Fernet)?
#
#   Fernet is simpler (no manual nonce) and has built-in TTL support, but it uses
#   AES-128-CBC + HMAC-SHA256 internally.  AESGCM uses AES-256-GCM — a single-pass
#   AEAD primitive that is both authenticated and encrypted, hardware-accelerated
#   on modern CPUs, and the NIST/IETF standard (RFC 5116).
#
# Key format: 32-byte random key stored as URL-safe base64 (44 chars with padding).
#   Generate once:  base64.urlsafe_b64encode(os.urandom(32)).decode()
#   Store in Databricks Secrets under SECRET_SCOPE / _TOKEN_CACHE_KEY_NAME.
#   Rotate manually — no code change needed; the next encrypt/decrypt call picks it up.
#
# Ciphertext format:  base64( 12-byte nonce ‖ ciphertext ‖ 16-byte GCM tag )
#   A fresh random nonce is generated per encrypt call (never reused).
#   The GCM tag provides tamper detection — decrypt raises InvalidTag on any
#   modification or wrong key.
#
# Lifetime:  the orchestrator encrypts once and writes to catalog.ingestion.token_cache.
#   Each entity extractor reads and decrypts from that table.  The row is overwritten
#   on the next batch run and updated in-place whenever an entity refreshes on a 401.
# ──────────────────────────────────────────────────────────────────────────────

def encrypt_token(token: str, log: "ContextLogger") -> str:
    """
    AES-256-GCM encrypt a raw token string.

    The AES key is read from Databricks Secrets — never embedded in SQL,
    widget parameters, or log lines.  The ciphertext (stored in
    catalog.ingestion.token_cache) is useless without the key.

    Returns base64(12-byte nonce ‖ ciphertext+tag), or empty string on failure
    so the caller can fall back to L3 (live token fetch) gracefully.
    """
    try:
        # Normalise padding: "+ '=' * (-len % 4)" adds exactly 0, 1, or 2 '='
        # chars to make the length a multiple of 4 — handles keys stored without
        # trailing '=' (e.g. stripped by some secret managers).
        _k     = dbutils.secrets.get(scope=SECRET_SCOPE, key=_TOKEN_CACHE_KEY_NAME)
        key    = base64.urlsafe_b64decode(_k + "=" * (-len(_k) % 4))
        nonce  = _os.urandom(12)                         # fresh 96-bit nonce per call
        ct_tag = _AESGCM(key).encrypt(nonce, token.encode(), None)
        result = base64.b64encode(nonce + ct_tag).decode()
        log.debug(f"Token encrypted  key={_TOKEN_CACHE_KEY_NAME}")
        return result
    except Exception as exc:
        log.warning(
            f"Token encryption failed — token cache write will be skipped, "
            f"entity extractors will use L3  error={exc}"
        )
        return ""


def decrypt_token(ciphertext: str, log: "ContextLogger") -> str:
    """
    Decrypt an AES-256-GCM ciphertext produced by encrypt_token().

    Returns the original plaintext token string.
    Raises cryptography.exceptions.InvalidTag if the ciphertext was tampered
    with or decrypted with the wrong key — caller should catch and fall back to L3.
    """
    # Same padding normalisation as encrypt_token.
    _k     = dbutils.secrets.get(scope=SECRET_SCOPE, key=_TOKEN_CACHE_KEY_NAME)
    key    = base64.urlsafe_b64decode(_k + "=" * (-len(_k) % 4))
    data   = base64.b64decode(ciphertext)
    nonce  = data[:12]           # first 12 bytes = nonce written by encrypt_token
    ct_tag = data[12:]           # remainder = ciphertext + 16-byte GCM auth tag
    token  = _AESGCM(key).decrypt(nonce, ct_tag, None).decode()
    log.debug(f"Token decrypted  key={_TOKEN_CACHE_KEY_NAME}")
    return token


def build_auth_headers_from_token(auth_type: str, token: str) -> dict:
    """
    Reconstruct the full headers dict from a raw token string.

    token is the value that follows "Bearer " or "Basic " in the Authorization
    header — i.e., the part stored by encrypt_token().  Source-specific headers
    (GitHub API version, Accept) are rebuilt here because they are constant for
    a given auth type.
    """
    auth_type = auth_type.upper()
    if auth_type in ("GITHUB_PAT", "GITHUB_APP"):
        return {
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": _GITHUB_API_VERSION,
        }
    else:
        # ADO_OAUTH, SNOW_OAUTH, OAUTH2, TOKEN — all use Bearer.
        # PAT and SNOW_BASIC use Basic auth and are excluded from the token cache
        # (_OAUTH_PREFETCH_TYPES), so they never reach this function.
        # If a new Basic-auth type is ever added to prefetch, add an explicit
        # branch above rather than letting it fall through here.
        return {"Authorization": f"Bearer {token}"}


# COMMAND ----------
# ── Shared Delta token cache ───────────────────────────────────────────────────
# catalog.ingestion.token_cache holds one row per source_name.
#
# Flow:
#   Orchestrator  — encrypts token once, calls write_token_cache to seed the row
#   Entity startup — calls read_token_cache; uses DB token if found/decryptable
#   On 401        — token_refresher (in entity extractor) sleeps 1–3 s, re-reads DB:
#                   if DB fetched_at is newer than what this entity loaded, another
#                   entity already refreshed → use that token (no L3 call needed).
#                   Otherwise fetch from L3, write to DB, continue.
#
# Race protection:
#   Jitter (1–3 s sleep) staggers simultaneous 401 handlers.  The MERGE always
#   writes (last valid writer wins) — safe because all freshly-fetched tokens are
#   valid; the "winner" just overwrites other equally-fresh tokens harmlessly.
# ──────────────────────────────────────────────────────────────────────────────

_TOKEN_CACHE_TABLE: str = "catalog.ingestion.token_cache"


def write_token_cache(source_name: str, encrypted_token: str, log: "ContextLogger") -> None:
    """
    Upsert (source_name, encrypted_token, fetched_at=current_timestamp()) into
    the shared Delta token cache.  Non-fatal — logs a warning on any error.
    """
    try:
        # source_name is validated in get_auth_headers to [A-Za-z0-9 \-_] only.
        # encrypted_token is AES-256-GCM base64 [A-Za-z0-9+/=] — no SQL injection
        # risk, but we defensively escape single quotes anyway.
        _s = source_name.replace("'", "''")
        _e = encrypted_token.replace("'", "''")
        spark.sql(f"""
            MERGE INTO {_TOKEN_CACHE_TABLE} AS t
            USING (
                SELECT '{_s}' AS source_name,
                       '{_e}' AS encrypted_token,
                       current_timestamp() AS fetched_at
            ) AS s
            ON t.source_name = s.source_name
            WHEN MATCHED THEN
                UPDATE SET t.encrypted_token = s.encrypted_token,
                           t.fetched_at      = s.fetched_at
            WHEN NOT MATCHED THEN
                INSERT (source_name, encrypted_token, fetched_at)
                VALUES (s.source_name, s.encrypted_token, s.fetched_at)
        """)
        log.debug(f"Token cache written  source={source_name}")
    except Exception as exc:
        log.warning(f"Token cache write failed  source={source_name}  error={exc}")


def read_token_cache(source_name: str, log: "ContextLogger"):
    """
    Read the cached token for source_name from the shared Delta table.
    Returns (encrypted_token, fetched_at) or ("", None) on any miss/error.
    fetched_at is returned as a timezone-aware UTC datetime.
    """
    try:
        from pyspark.sql.functions import col as _col
        rows = (
            spark.read.table(_TOKEN_CACHE_TABLE)
            .filter(_col("source_name") == source_name)
            .select("encrypted_token", "fetched_at")
            .limit(1)
            .collect()
        )
        if rows:
            _enc = rows[0]["encrypted_token"]
            _ft  = rows[0]["fetched_at"]
            if _ft is not None and _ft.tzinfo is None:
                _ft = _ft.replace(tzinfo=_timezone.utc)
            return _enc or "", _ft
    except Exception as exc:
        log.warning(f"Token cache read failed  source={source_name}  error={exc}")
    return "", None


# COMMAND ----------

# DBTITLE 1,http_client
# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/http_client
# MAGIC Shared HTTP session factory and rate-limit-aware GET/POST helpers.
# MAGIC Depends on: `logging_utils` (must be `%run` first).

# COMMAND ----------
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
import urllib3
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# urllib3 2.x renamed method_whitelist → allowed_methods.
# Guarded so a non-standard version string never crashes module load.
try:
    _RETRY_METHODS_KWARG = (
        "allowed_methods" if int(urllib3.__version__.split(".")[0]) >= 2
        else "method_whitelist"
    )
except Exception:
    _RETRY_METHODS_KWARG = "allowed_methods"   # safe default for Databricks 13+

# ── Constants ──────────────────────────────────────────────────────────────────
TIMEOUT        = (5, 60)   # (connect_timeout_sec, read_timeout_sec)
MAX_RETRIES    = 3         # urllib3 automatic retries for transient 5xx / network errors
BACKOFF_FACTOR = 2.0       # sleep = backoff_factor * (2 ** (attempt - 1))  → 2, 4, 8 s
MAX_429_WAITS  = 5         # max request attempts for 429; Retry-After + jitter honoured
                           # between each attempt (max_waits - 1 sleeps total)

# 5xx codes retried automatically by urllib3 for GET; handled manually in safe_post.
# 429 is excluded from both — handled by safe_get/safe_post so Retry-After is respected.
_RETRY_ON_STATUS: frozenset[int] = frozenset({500, 502, 503, 504})

# Network exceptions that warrant a retry in safe_post (urllib3 retries these for
# GET automatically, but not for POST since POST is excluded from allowed_methods).
_NETWORK_ERRORS = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)

# Hard cap on Retry-After values.  Prevents a misbehaving server from sending
# Retry-After: 86400 and parking the job for a day.
_MAX_RETRY_AFTER: int = 600   # 10 minutes


def _parse_retry_after(resp: "Response", default: int = 60) -> int:
    """
    Parse the Retry-After header from a 429 / 503 response.

    Supports both formats the HTTP spec allows:
      - Seconds:   "30"  or  "30.5"  (float strings are rounded)
      - HTTP-date: "Thu, 16 Sep 2026 10:00:00 GMT"

    Returns seconds to wait as an int, clamped to [1, _MAX_RETRY_AFTER].
    Falls back to `default` on any parse error.
    """
    value = resp.headers.get("Retry-After", "")
    if not value:
        return default
    try:
        # float() before int() so "30.5" parses as 30 rather than falling
        # through to the HTTP-date parser and returning the 60 s default.
        # min/max: floor at 1 (Retry-After: 0 would cause a hot-retry loop),
        # cap at _MAX_RETRY_AFTER (guard against "Retry-After: 86400").
        return min(max(int(float(value)), 1), _MAX_RETRY_AFTER)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        wait = int((retry_at - datetime.now(timezone.utc)).total_seconds())
        return min(max(wait, 1), _MAX_RETRY_AFTER)
    except Exception:
        return default

# COMMAND ----------

def build_http_session(
    retries: int   = MAX_RETRIES,
    backoff: float = BACKOFF_FACTOR,
) -> Session:
    """
    Return a requests.Session with automatic retry and connection pooling.

    urllib3 handles transient 5xx errors on GET with exponential back-off.
    429 rate-limit responses are NOT in status_forcelist — they are handled
    by safe_get / safe_post so the Retry-After header value is respected.
    POST is excluded from urllib3 auto-retry; safe_post handles it manually.
    """
    retry = Retry(
        total            = retries,
        backoff_factor   = backoff,
        status_forcelist = _RETRY_ON_STATUS,
        raise_on_status  = False,    # status is inspected in safe_get / safe_post
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
    max_waits:       int   = MAX_429_WAITS,
    token_refresher        = None,
    timeout                = TIMEOUT,
) -> Response:
    """
    GET with explicit 429 / Retry-After back-off on top of urllib3 5xx retry.

    Args:
        session:         Shared requests.Session (connection-pooled, auto-retries 5xx).
        url:             Fully-qualified URL.
        headers:         Auth + Accept headers.  Mutated in-place on token refresh so
                         all subsequent pages in the same paginator use the new token.
        params:          Query-string parameters.
        log:             ContextLogger from logging_utils.
        max_waits:       Max request attempts before giving up on 429.
        token_refresher: Optional callable () -> dict of fresh auth headers.
                         A single 401 triggers one refresh + one retry.
                         A second 401 raises HTTPError immediately.
        timeout:         Override TIMEOUT for slow endpoints  e.g. (5, 120).

    Returns:
        requests.Response with a 2xx status code.

    Raises:
        RuntimeError:       if 429 persists beyond max_waits.
        requests.HTTPError: for non-retriable 4xx / 5xx after all retries.
    """
    _token_refreshed = False
    for attempt in range(1, max_waits + 1):
        resp = session.get(url, headers=headers, params=params, timeout=timeout)

        if resp.status_code == 429:
            wait = _parse_retry_after(resp)
            log.warning(
                f"Rate-limited  attempt={attempt}/{max_waits}"
                f"  retry-after={wait}s  url={url}"
            )
            if attempt < max_waits:
                # ±10% jitter staggers concurrent entity extractors that all
                # hit ADO's org-level rate limit at the same instant.
                time.sleep(wait + random.uniform(0, wait * 0.1))
            continue

        if resp.status_code == 401 and token_refresher is not None and not _token_refreshed:
            _token_refreshed = True
            log.warning(f"401 Unauthorized — refreshing token and retrying once  url={url}")
            headers.update(token_refresher())
            resp = session.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 429:
                # Token expiry coinciding with a rate-limit burst is a real
                # production scenario.  Re-enter the loop instead of raising so
                # the remaining retry budget is not thrown away.
                wait = _parse_retry_after(resp)
                log.warning(
                    f"429 after token refresh  retry-after={wait}s  url={url}"
                )
                if attempt < max_waits:
                    time.sleep(wait + random.uniform(0, wait * 0.1))
                continue
            # Second 401 after refresh → raise immediately; do not retry again.
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
    max_waits:       int   = MAX_429_WAITS,
    token_refresher        = None,
    timeout                = TIMEOUT,
) -> Response:
    """
    POST with the same 429 / Retry-After and 401 refresh logic as safe_get.

    Intended for read-only POST endpoints (e.g. ADO WIQL) where retrying is safe.
    urllib3's automatic 5xx / network retry does NOT cover POST (excluded from
    allowed_methods), so this function provides equivalent manual retry for
    429, 5xx, and transient network errors (ConnectionError / Timeout).

    Args:
        body:    JSON-serialisable dict sent as the request body.
        timeout: Override TIMEOUT for slow WIQL queries  e.g. (5, 90).
        (other args: same meaning as safe_get)

    Returns:
        requests.Response with a 2xx status code.
    """
    _token_refreshed = False
    for attempt in range(1, max_waits + 1):

        # ── Initial request ───────────────────────────────────────────────────
        # Catch transient network errors — urllib3 retries these for GET
        # automatically but POST is excluded from allowed_methods.
        try:
            resp = session.post(url, headers=headers, params=params,
                                json=body, timeout=timeout)
        except _NETWORK_ERRORS as exc:
            if attempt < max_waits:
                sleep = BACKOFF_FACTOR * (2 ** (attempt - 1))
                log.warning(
                    f"Network error (POST)  attempt={attempt}/{max_waits}"
                    f"  sleeping={round(sleep, 1)}s  url={url}  error={exc}"
                )
                time.sleep(sleep + random.uniform(0, sleep * 0.1))
                continue
            raise

        # ── 429 ───────────────────────────────────────────────────────────────
        if resp.status_code == 429:
            wait = _parse_retry_after(resp)
            log.warning(
                f"Rate-limited (POST)  attempt={attempt}/{max_waits}"
                f"  retry-after={wait}s  url={url}"
            )
            if attempt < max_waits:
                time.sleep(wait + random.uniform(0, wait * 0.1))
            continue

        # ── 5xx ───────────────────────────────────────────────────────────────
        # urllib3 does not auto-retry POST; honour Retry-After when present,
        # fall back to exponential back-off with jitter.
        if resp.status_code in _RETRY_ON_STATUS:
            sleep = _parse_retry_after(
                resp, default=max(1, int(BACKOFF_FACTOR * (2 ** (attempt - 1))))
            )
            log.warning(
                f"Server error (POST)  attempt={attempt}/{max_waits}"
                f"  status={resp.status_code}  sleeping={round(sleep, 1)}s  url={url}"
            )
            if attempt < max_waits:
                time.sleep(sleep + random.uniform(0, sleep * 0.1))
            continue

        # ── 401 token refresh ─────────────────────────────────────────────────
        if resp.status_code == 401 and token_refresher is not None and not _token_refreshed:
            _token_refreshed = True
            log.warning(f"401 Unauthorized (POST) — refreshing token and retrying once  url={url}")
            headers.update(token_refresher())
            try:
                resp = session.post(url, headers=headers, params=params,
                                    json=body, timeout=timeout)
            except _NETWORK_ERRORS as exc:
                if attempt < max_waits:
                    sleep = BACKOFF_FACTOR * (2 ** (attempt - 1))
                    log.warning(
                        f"Network error after token refresh (POST)  attempt={attempt}/{max_waits}"
                        f"  sleeping={round(sleep, 1)}s  url={url}  error={exc}"
                    )
                    time.sleep(sleep + random.uniform(0, sleep * 0.1))
                    continue
                raise
            if resp.status_code == 429:
                wait = _parse_retry_after(resp)
                log.warning(
                    f"429 after token refresh (POST)  retry-after={wait}s  url={url}"
                )
                if attempt < max_waits:
                    time.sleep(wait + random.uniform(0, wait * 0.1))
                continue
            if resp.status_code in _RETRY_ON_STATUS:
                sleep = _parse_retry_after(
                    resp, default=max(1, int(BACKOFF_FACTOR * (2 ** (attempt - 1))))
                )
                log.warning(
                    f"Server error after token refresh (POST)"
                    f"  status={resp.status_code}  sleeping={round(sleep, 1)}s  url={url}"
                )
                if attempt < max_waits:
                    time.sleep(sleep + random.uniform(0, sleep * 0.1))
                continue
            # Second 401 after refresh → raise immediately.
            resp.raise_for_status()
            return resp

        resp.raise_for_status()
        return resp

    raise RuntimeError(
        f"Retries exhausted after {max_waits} attempts (POST)  url={url}"
    )


# COMMAND ----------

# DBTITLE 1,landing_writer_spark
# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/landing_writer_spark
# MAGIC Spark-based variant of the landing writer.  Writes each chunk as a
# MAGIC **JSON Lines** partition via `spark.write.json()` so the path can be any
# MAGIC scheme Spark supports natively — including Azure ADLS Gen2 (`abfss://`),
# MAGIC AWS S3 (`s3://`), GCS (`gs://`), DBFS (`dbfs:/`), and Unity Catalog
# MAGIC volumes (`/Volumes/`).
# MAGIC
# MAGIC **When to use this file vs landing_writer**
# MAGIC   - Use this file when `landing_zone_path` is an Azure ADLS Gen2 URI
# MAGIC     (`abfss://container@storage.dfs.core.windows.net/path`) or any other
# MAGIC     cloud-storage scheme the driver's FUSE mount cannot reach.
# MAGIC   - Use `landing_writer` (Python I/O) for `dbfs:/` and `/Volumes/` paths
# MAGIC     where no Spark overhead is needed and type-exact raw records matter.
# MAGIC
# MAGIC **Switching**
# MAGIC Replace `%run ./_lib/landing_writer` with
# MAGIC `%run ./_lib/landing_writer_spark` in `01_Entity_Extractor`.
# MAGIC The public function `write_pages_to_json` has the same signature in both
# MAGIC files, so no other change is required.
# MAGIC
# MAGIC **Output files**
# MAGIC Each chunk calls `coalesce(1).write.mode("append").json(path)`, producing
# MAGIC one `part-00000-<uuid>.json` file per chunk.  Spark generates the UUID
# MAGIC suffix; the landing path already contains the run_id so filenames are
# MAGIC globally unique across runs.
# MAGIC
# MAGIC **Schema inference**
# MAGIC `createDataFrame(records)` infers column types from the Python values.
# MAGIC JSON-native types (str, int, float, bool, None, list, dict) round-trip
# MAGIC cleanly.  If a source field mixes types across pages (e.g. sometimes int,
# MAGIC sometimes str) Spark will widen the inferred type.  Silver-layer jobs
# MAGIC should read with `mergeSchema=true` and cast explicitly.
# MAGIC
# MAGIC **Memory bound**
# MAGIC Same as landing_writer: at most `_CHUNK_RECORDS` rows in driver memory
# MAGIC before a flush; oversized pages are split by an inner `while` loop.
# MAGIC
# MAGIC **Write retries**
# MAGIC Each chunk retries up to `_WRITE_RETRIES` times on any exception
# MAGIC (ADLS connection resets, transient 503s from storage, Spark task failures)
# MAGIC with exponential back-off.  Non-retryable failures (e.g. permission denied,
# MAGIC bad path) re-raise immediately after the final attempt so the entity
# MAGIC extractor closes the audit row as FAILED.
# MAGIC
# MAGIC **Reading back for the watermark advance (in 01_Entity_Extractor)**
# MAGIC   spark.read.option("mergeSchema", "true").json(landing_path)
# MAGIC Spark reads the abfss:// URI directly with the cluster's service-principal
# MAGIC credentials — no extra configuration needed if the cluster already has
# MAGIC read/write access to the storage account.
# MAGIC
# MAGIC **Watermark boundary overlap**
# MAGIC All paginators use `>=` (ge), so the record at the exact max-watermark
# MAGIC timestamp from run N is re-fetched on run N+1.  Silver-layer jobs must
# MAGIC deduplicate on the entity primary key before aggregating.
# MAGIC
# MAGIC Depends on: `logging_utils` (must be `%run` first).

# COMMAND ----------
import time as _time

_CHUNK_RECORDS = 50_000   # flush buffer after this many records (~50 MB at 1 KB/record)
_WRITE_RETRIES = 3        # write attempts per chunk before giving up
_WRITE_BACKOFF = 2.0      # back-off base in seconds → sleeps: 2 s, 4 s between retries

# COMMAND ----------


def _write_chunk(
    spark,
    records:   list,
    path:      str,
    chunk_num: int,
    log:       "ContextLogger",
) -> None:
    """
    Write `records` as a single-partition JSON file appended to `path`.

    Uses coalesce(1) so exactly one part file is produced per chunk.
    Retries up to _WRITE_RETRIES times on any exception with exponential
    back-off.  Raises RuntimeError (wrapping the last exception) when all
    attempts fail.
    """
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(1, _WRITE_RETRIES + 1):
        try:
            (
                spark.createDataFrame(records)
                     .coalesce(1)
                     .write
                     .mode("append")
                     .json(path)
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


def write_pages_to_json(
    page_iter:    "Iterator[list[dict]]",
    landing_path: str,
    log:          "ContextLogger",
    spark=None,
) -> int:
    """
    Consume the page generator, write records to JSON in fixed-size chunks via Spark.

    Each chunk appends one `part-00000-<uuid>.json` file to `landing_path`.
    Handles any path scheme Spark supports: abfss://, dbfs:/, s3://, /Volumes/.

    Args:
        page_iter:    Generator from a PAGINATION_DISPATCH handler.
        landing_path: Destination directory — any Spark-compatible URI.
        log:          ContextLogger from logging_utils.
        spark:        Active SparkSession.  Must not be None.

    Returns:
        Total number of records written.

    Raises:
        ValueError:   if spark is None.
        RuntimeError: if a chunk write fails after all retries.
    """
    if spark is None:
        raise ValueError(
            "write_pages_to_json (Spark variant) requires a SparkSession. "
            "Pass spark=spark when calling, or switch to landing_writer for "
            "DBFS / Unity Catalog volume paths that don't need Spark."
        )

    buffer    = []
    total     = 0
    chunk_num = 0

    for page_num, batch in enumerate(page_iter):
        buffer.extend(batch)
        log.debug(
            f"Page buffered  page={page_num + 1}"
            f"  size={len(batch)}"
            f"  buffer={len(buffer)}"
        )

        # `while` not `if`: splits a single oversized page into multiple chunks
        # so driver memory never holds more than _CHUNK_RECORDS rows at once.
        while len(buffer) >= _CHUNK_RECORDS:
            chunk_buf  = buffer[:_CHUNK_RECORDS]
            buffer     = buffer[_CHUNK_RECORDS:]
            chunk_num += 1
            _write_chunk(spark, chunk_buf, landing_path, chunk_num, log)
            total     += len(chunk_buf)
            log.debug(f"Chunk flushed  chunk={chunk_num}  running_total={total}")

    if buffer:
        chunk_num += 1
        _write_chunk(spark, buffer, landing_path, chunk_num, log)
        total     += len(buffer)
        log.debug(f"Chunk flushed  chunk={chunk_num}  running_total={total}")

    if total == 0:
        log.info("No records returned by API — landing zone not written")
        return 0

    log.info(
        f"JSON write complete (Spark)"
        f"  records={total}"
        f"  chunks={chunk_num}"
        f"  path={landing_path}"
    )
    return total


# COMMAND ----------

# DBTITLE 1,landing_writer
# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/landing_writer
# MAGIC Buffers paginated API records in fixed-size chunks and writes each chunk
# MAGIC as a **JSON Lines** (`.jsonl`) file — one JSON object per line, exactly as
# MAGIC received from the source API.  No schema inference or type coercion.
# MAGIC
# MAGIC **When to use this file vs landing_writer_spark**
# MAGIC   - Use this file when `landing_zone_path` is `dbfs:/…` or `/Volumes/…`.
# MAGIC   - Use `landing_writer_spark` when the path is an Azure ADLS Gen2 direct
# MAGIC     URI (`abfss://…`) or any other cloud-storage scheme that Spark handles
# MAGIC     natively but the driver's FUSE mount does not.
# MAGIC
# MAGIC **Supported path schemes**
# MAGIC   - `dbfs:/…`     → FUSE-mounted at `/dbfs/…`; written by the driver
# MAGIC   - `/Volumes/…`  → Unity Catalog volume; direct driver access
# MAGIC   - Absolute paths (e.g. `/tmp/…`) → driver-local (dev / testing only)
# MAGIC
# MAGIC **Memory bound**
# MAGIC At most `_CHUNK_RECORDS` rows are held in driver memory before a flush.
# MAGIC A single oversized page is split by the inner `while` loop so the bound
# MAGIC holds even when one API page exceeds the chunk size.
# MAGIC
# MAGIC **Write retries**
# MAGIC Each chunk retries up to `_WRITE_RETRIES` times on `OSError` with
# MAGIC exponential back-off.  An unrecoverable failure raises `RuntimeError`
# MAGIC (wrapping the original `OSError`) so the entity extractor closes the audit
# MAGIC row as FAILED and the workflow can schedule a clean retry.
# MAGIC
# MAGIC **Reading back for the watermark advance (in 01_Entity_Extractor)**
# MAGIC   spark.read.option("mergeSchema", "true").json(landing_path)
# MAGIC Spark's JSON reader accepts the original `dbfs:/` URI directly — no FUSE
# MAGIC conversion is needed on the read side.
# MAGIC
# MAGIC **Watermark boundary overlap**
# MAGIC All paginators use `>=` (ge), so the record at the exact max-watermark
# MAGIC timestamp from run N is re-fetched on run N+1.  Silver-layer jobs must
# MAGIC deduplicate on the entity primary key before aggregating.
# MAGIC
# MAGIC Depends on: `logging_utils` (must be `%run` first).

# COMMAND ----------
import json as _json
import os   as _os
import time as _time

_CHUNK_RECORDS = 50_000   # flush buffer after this many records (~50 MB at 1 KB/record)
_WRITE_RETRIES = 3        # write attempts per chunk before giving up
_WRITE_BACKOFF = 2.0      # back-off base in seconds → sleeps: 2 s, 4 s between retries

# COMMAND ----------


def _to_local_path(path: str) -> str:
    """
    Normalise a DBFS URI or volume path to an absolute local path for Python I/O.

        dbfs:/a/b        →  /dbfs/a/b
        /Volumes/c/s/v   →  /Volumes/c/s/v   (pass-through)
        /absolute/path   →  /absolute/path   (pass-through)

    Raises ValueError for unsupported URI schemes (abfss://, s3://, gs://).
    Use landing_writer_spark for those paths.
    """
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[6:].lstrip("/")
    if path.startswith("/"):
        return path
    raise ValueError(
        f"landing_path '{path}' uses an unsupported URI scheme for Python I/O. "
        "Supported: dbfs:/, /Volumes/..., or an absolute local path. "
        "For Azure ADLS (abfss://), S3 (s3://), or GCS (gs://) URIs "
        "use landing_writer_spark instead."
    )


def _write_chunk(
    records:   list,
    directory: str,
    chunk_num: int,
    log:       "ContextLogger",
) -> None:
    """
    Serialise `records` as JSON Lines and write to `directory/part-NNNNN.jsonl`.

    Retries up to _WRITE_RETRIES times on OSError with exponential back-off.
    Raises RuntimeError (wrapping the last OSError) when all attempts fail.
    """
    local_dir = _to_local_path(directory)
    _os.makedirs(local_dir, exist_ok=True)
    file_path = _os.path.join(local_dir, f"part-{chunk_num:05d}.jsonl")

    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(1, _WRITE_RETRIES + 1):
        try:
            with open(file_path, "w", encoding="utf-8") as fh:
                for record in records:
                    fh.write(_json.dumps(record, default=str))
                    fh.write("\n")
            log.debug(
                f"Chunk written"
                f"  chunk={chunk_num}"
                f"  records={len(records)}"
                f"  file={file_path}"
            )
            return
        except OSError as exc:
            last_exc = exc
            if attempt < _WRITE_RETRIES:
                sleep = _WRITE_BACKOFF * (2 ** (attempt - 1))
                log.warning(
                    f"Chunk write failed — retrying"
                    f"  attempt={attempt}/{_WRITE_RETRIES}"
                    f"  sleeping={sleep}s"
                    f"  file={file_path}"
                    f"  error={exc}"
                )
                _time.sleep(sleep)

    raise RuntimeError(
        f"Chunk write failed after {_WRITE_RETRIES} attempts"
        f"  file={file_path}"
        f"  error={last_exc}"
    ) from last_exc


def write_pages_to_json(
    page_iter:    "Iterator[list[dict]]",
    landing_path: str,
    log:          "ContextLogger",
    spark=None,                           # accepted but unused; keeps signature compatible with landing_writer_spark
) -> int:
    """
    Consume the page generator, write records to JSON Lines in fixed-size chunks.

    Each chunk produces one `part-NNNNN.jsonl` file inside `landing_path`.
    For most incremental runs the threshold is never hit and one file is written.

    Args:
        page_iter:    Generator from a PAGINATION_DISPATCH handler.
        landing_path: Destination directory — dbfs:/ URI, /Volumes/..., or absolute path.
        log:          ContextLogger from logging_utils.
        spark:        Ignored (present for API compatibility with landing_writer_spark).

    Returns:
        Total number of records written.

    Raises:
        ValueError:   if landing_path uses an unsupported URI scheme.
        RuntimeError: if a chunk write fails after all retries.
    """
    # Validate path scheme before any paginator / network work so a
    # misconfigured path fails fast with a clear message.
    _to_local_path(landing_path)

    buffer    = []
    total     = 0
    chunk_num = 0

    for page_num, batch in enumerate(page_iter):
        buffer.extend(batch)
        log.debug(
            f"Page buffered  page={page_num + 1}"
            f"  size={len(batch)}"
            f"  buffer={len(buffer)}"
        )

        # `while` not `if`: splits a single oversized page into multiple chunks
        # so driver memory never holds more than _CHUNK_RECORDS rows at once.
        while len(buffer) >= _CHUNK_RECORDS:
            chunk_buf  = buffer[:_CHUNK_RECORDS]
            buffer     = buffer[_CHUNK_RECORDS:]
            chunk_num += 1
            _write_chunk(chunk_buf, landing_path, chunk_num, log)
            total     += len(chunk_buf)
            log.debug(f"Chunk flushed  chunk={chunk_num}  running_total={total}")

    if buffer:
        chunk_num += 1
        _write_chunk(buffer, landing_path, chunk_num, log)
        total     += len(buffer)
        log.debug(f"Chunk flushed  chunk={chunk_num}  running_total={total}")

    if total == 0:
        log.info("No records returned by API — landing zone not written")
        return 0

    log.info(
        f"JSON Lines write complete"
        f"  records={total}"
        f"  chunks={chunk_num}"
        f"  path={landing_path}"
    )
    return total


# COMMAND ----------

# DBTITLE 1,logging_utils
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
        fmt="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
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
        ctx = " ".join(f"{k}={v}" for k, v in self.extra.items() if v)
        return (f"[{ctx}] {msg}" if ctx else msg), kwargs


# COMMAND ----------

# DBTITLE 1,paginators
# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/paginators_ado
# MAGIC Azure DevOps pagination handlers.
# MAGIC
# MAGIC Exports `PAGINATION_DISPATCH_ADO` — a dict of pagination_type → generator
# MAGIC function for all ADO-specific pagination styles.
# MAGIC
# MAGIC Loaded by `_lib/paginators` (the aggregator).  Can also be `%run` directly
# MAGIC in a source-specific extractor notebook.
# MAGIC
# MAGIC Depends on: `logging_utils`, `http_client` (must be `%run` first).

# COMMAND ----------
from typing import Iterator, Optional

_PAGE_SIZE       = 100     # records per API request
_MAX_PAGES       = 10_000  # safety cap: prevents infinite loops on misbehaving APIs
_ADO_API_VERSION = "7.1"
_WIQL_BATCH_SIZE = 200     # ADO hard limit: max 200 IDs per GET /_apis/wit/workitems


def _to_snow_dt(val: str) -> str:
    """
    Convert an ISO-8601 datetime to 'YYYY-MM-DD HH:MM:SS' format.
    WIQL datetime literals require a space separator — no T, no Z, no offset suffix.
    Safe to call on already-space-format input.
    Negative offsets (e.g. -05:00) are stripped by the final [:19] slice,
    not by split("+")[0] which only handles positive offsets.
    """
    return val.replace("T", " ").split("+")[0].rstrip("Z").split(".")[0][:19]

# COMMAND ----------


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
    Follow ADO's x-ms-continuationtoken header until absent.

    Typical sources: Azure DevOps REST API  (/_apis/wit/workitems …)

    ingestion_entity_config setup:
        pagination_type        = ContinuationToken
        endpoint_url           = /_apis/wit/workitems
        watermark_column_name  = System.ChangedDate
        watermark_column_value = <ISO-8601 timestamp e.g. 2020-01-01T00:00:00Z>

    Yields:
        list[dict] — one page of raw ADO records.
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
        token   = resp.headers.get("x-ms-continuationtoken")
        page   += 1

        if page >= _MAX_PAGES:
            log.warning(
                f"ContinuationToken reached _MAX_PAGES={_MAX_PAGES} safety limit — stopping. "
                "Increase _MAX_PAGES in paginators_ado.py if the API genuinely has this many pages."
            )
            return

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

    NOTE — ADO Analytics field names differ from ADO REST:
        REST (System.ChangedDate) → Analytics (ChangedDate)
    Set watermark_column_name to the Analytics name (no "System." prefix).

    ingestion_entity_config setup:
        pagination_type        = OData
        endpoint_url           = /_odata/v4.0/WorkItems
        watermark_column_name  = ChangedDate
        watermark_column_value = <ISO-8601 timestamp e.g. 2026-01-01T00:00:00Z>

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
        resp     = safe_get(session, url, headers, params, log, token_refresher=token_refresher)
        body     = resp.json()
        batch    = body.get("value", [])
        next_url = body.get("@odata.nextLink")
        page    += 1

        if page >= _MAX_PAGES:
            log.warning(
                f"OData reached _MAX_PAGES={_MAX_PAGES} safety limit — stopping. "
                "Increase _MAX_PAGES in paginators_ado.py if the API genuinely has this many pages."
            )
            return

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

        # Follow nextLink as-is — it already carries $skiptoken and all other params.
        # params={} prevents requests from appending duplicates.
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
    Azure DevOps WIQL two-step pagination.

    Step 1: POST /_apis/wit/wiql — returns a flat list of matching work item IDs.
    Step 2: GET /_apis/wit/workitems?ids=<batch> — fetches fields in batches of 200.

    ── pagination_config keys ────────────────────────────────────────────────
    wiql_where  STRING  WIQL WHERE body.  The watermark condition is appended
                        automatically.  Wrap field names in [ ].
                        Default: "[System.WorkItemType] = 'Epic'"

    fields      LIST    ADO field reference names for Step 2.
                        Omit for $expand=all (convenient but slow — avoid in prod).

    ── Example configs ──────────────────────────────────────────────────────
    Epics only (default):
        {"wiql_where": "[System.WorkItemType] = 'Epic'"}

    Features with explicit fields (recommended):
        {"wiql_where": "[System.WorkItemType] = 'Feature'",
         "fields": ["System.Id","System.Title","System.State","System.ChangedDate"]}

    ingestion_entity_config setup:
        pagination_type        = WIQL
        endpoint_url           = /_apis/wit/workitems
        watermark_column_name  = System.ChangedDate
        watermark_column_value = <ISO-8601 timestamp e.g. 2020-01-01T00:00:00Z>
        pagination_config      = {"wiql_where": "..."}

    Yields:
        list[dict] — one batch of work item records.
    """
    import json as _json

    cfg_dict: dict         = _json.loads(pagination_config) if pagination_config else {}
    wiql_where: str        = cfg_dict.get("wiql_where", "[System.WorkItemType] = 'Epic'")
    fields: Optional[list] = cfg_dict.get("fields")

    # ── Step 1: POST WIQL ─────────────────────────────────────────────────
    # WIQL requires space-format datetime ('YYYY-MM-DD HH:MM:SS'), not ISO-8601.
    wiql_url  = f"{base_url}/_apis/wit/wiql"
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

    if len(all_ids) == 20_000:
        log.warning(
            "WIQL result hit the $top=20000 cap — items beyond 20000 are silently excluded. "
            "Narrow wiql_where or split into multiple entity configs."
        )

    log.info(
        f"WIQL  where={wiql_where!r}"
        f"  {wm_col}>={wm_val}"
        f"  ids_returned={len(all_ids)}"
    )

    if not all_ids:
        return

    # ── Step 2: GET field data in batches of 200 (ADO hard limit) ─────────
    details_url   = f"{base_url}{endpoint}"
    total_batches = (len(all_ids) + _WIQL_BATCH_SIZE - 1) // _WIQL_BATCH_SIZE

    for batch_num, start in enumerate(range(0, len(all_ids), _WIQL_BATCH_SIZE), 1):
        batch_ids = all_ids[start : start + _WIQL_BATCH_SIZE]
        params: dict = {
            "ids":         ",".join(str(i) for i in batch_ids),
            "api-version": _ADO_API_VERSION,
        }
        if fields:
            params["fields"] = ",".join(fields)
        else:
            params["$expand"] = "all"

        resp  = safe_get(session, details_url, headers, params, log,
                         token_refresher=token_refresher)
        batch = resp.json().get("value", [])

        if not batch:
            log.warning(
                f"WIQL  batch={batch_num}/{total_batches}"
                f"  ids={len(batch_ids)}  records=0"
                f"  — items may have been deleted between WIQL query and fetch"
            )
            continue

        log.debug(
            f"WIQL  batch={batch_num}/{total_batches}"
            f"  ids={len(batch_ids)}  records={len(batch)}"
        )

        yield batch

# COMMAND ----------

PAGINATION_DISPATCH_ADO: dict = {
    "ContinuationToken": paginate_by_continuation_token,
    "OData":             paginate_by_odata,
    "WIQL":              paginate_by_wiql,
}



# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/paginators_github
# MAGIC GitHub REST API pagination handlers.
# MAGIC
# MAGIC Exports `PAGINATION_DISPATCH_GITHUB` — a dict of pagination_type → generator
# MAGIC function for GitHub-specific pagination styles.
# MAGIC
# MAGIC Loaded by `_lib/paginators` (the aggregator).  Can also be `%run` directly
# MAGIC in a source-specific extractor notebook.
# MAGIC
# MAGIC Depends on: `logging_utils`, `http_client` (must be `%run` first).

# COMMAND ----------
import re as _re
from typing import Iterator, Optional

_PAGE_SIZE = 100     # records per API request
_MAX_PAGES = 10_000  # safety cap: prevents infinite loops on misbehaving APIs


def _parse_link_next(link_header: str) -> Optional[str]:
    """
    Parse the rel="next" URL from a GitHub-style Link response header.

    Example header value:
        <https://api.github.com/repos/octo/hello/issues?page=2>; rel="next",
        <https://api.github.com/repos/octo/hello/issues?page=5>; rel="last"

    Uses regex so URLs containing commas (cursor tokens, base64 params) are
    handled correctly per RFC 8288 — commas inside <URL> angle brackets are valid.

    Returns the next-page URL string, or None when rel="next" is absent.
    """
    if not link_header:
        return None
    for m in _re.finditer(r'<([^>]+)>[^,]*?\brel="next"', link_header):
        return m.group(1)
    return None

# COMMAND ----------


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
    Follow the rel="next" URL from the Link response header until absent.

    Recommended for all GitHub REST API list endpoints (issues, pulls, commits …)
    Preferred over PageNumber because:
    ─ No wasted last-page call (stops as soon as rel="next" is absent).
    ─ Correct under concurrent writes — Link URL encodes a cursor, not a raw
      page number, so records created mid-fetch don't cause duplicates/gaps.

    Response body shapes supported:
        [...]               GitHub list endpoints (issues, pulls, commits …)
        {"items": [...]}    GitHub search endpoints (/search/issues …)
        {"value": [...]}    OData-style fallback

    watermark_column_name must match GitHub's filter parameter:
        issues / pull requests / commits  →  "since"

    ingestion_entity_config setup:
        pagination_type        = LinkHeader
        endpoint_url           = /repos/{owner}/{repo}/issues
        watermark_column_name  = since
        watermark_column_value = <ISO-8601 timestamp e.g. 2026-01-01T00:00:00Z>

    Yields:
        list[dict] — one page of raw GitHub records.
    """
    url    = f"{base_url}{endpoint}"
    page   = 0
    params = {
        "per_page": _PAGE_SIZE,
        wm_col:     wm_val,
    }

    while True:
        resp = safe_get(session, url, headers, params, log, token_refresher=token_refresher)
        body = resp.json()

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

        next_url = _parse_link_next(resp.headers.get("Link", ""))
        page    += 1

        if page >= _MAX_PAGES:
            log.warning(
                f"LinkHeader reached _MAX_PAGES={_MAX_PAGES} safety limit — stopping. "
                "Increase _MAX_PAGES in paginators_github.py if the API genuinely has this many pages."
            )
            return

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

        # Follow the Link header URL exactly — it already carries per_page and cursor.
        # params={} prevents requests from appending duplicates.
        url    = next_url
        params = {}


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

    NOTE — for GitHub, prefer LinkHeader (paginate_by_link_header) instead.
    PageNumber is kept for non-GitHub APIs that use explicit page numbers and
    don't emit a Link header.

    The watermark_column_name is used directly as a query-string key, so its
    value in ingestion_entity_config must match the API's filter param name
    (e.g. "since" for GitHub issues).

    Yields:
        list[dict] — one page of raw records.
    """
    url  = f"{base_url}{endpoint}"
    page = 1

    while True:
        if page >= _MAX_PAGES:
            log.warning(
                f"PageNumber reached _MAX_PAGES={_MAX_PAGES} safety limit — stopping. "
                "Increase _MAX_PAGES in paginators_github.py if the API genuinely has this many pages."
            )
            return
        params = {
            "per_page": _PAGE_SIZE,
            "page":     page,
            wm_col:     wm_val,
        }
        resp  = safe_get(session, url, headers, params, log, token_refresher=token_refresher)
        body  = resp.json()

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

# COMMAND ----------

PAGINATION_DISPATCH_GITHUB: dict = {
    "LinkHeader": paginate_by_link_header,
    "PageNumber": paginate_by_page_number,
}



# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/paginators_servicenow
# MAGIC ServiceNow Table API pagination handler.
# MAGIC
# MAGIC Exports `PAGINATION_DISPATCH_SERVICENOW` — a dict of pagination_type →
# MAGIC generator function for ServiceNow-specific pagination.
# MAGIC
# MAGIC For compound ServiceNow queries (active=true^category=hardware combined with
# MAGIC a watermark) or custom field selection, use the Descriptor paginator with
# MAGIC filter_mode="sysparm" instead of OffsetPagination.
# MAGIC
# MAGIC Loaded by `_lib/paginators` (the aggregator).  Can also be `%run` directly
# MAGIC in a source-specific extractor notebook.
# MAGIC
# MAGIC Depends on: `logging_utils`, `http_client` (must be `%run` first).

# COMMAND ----------
from typing import Iterator

_PAGE_SIZE = 100     # records per API request
_MAX_PAGES = 10_000  # safety cap: prevents infinite loops on misbehaving APIs


def _to_snow_dt(val: str) -> str:
    """
    Convert an ISO-8601 datetime to ServiceNow Table API format.
    sysparm_query expects 'YYYY-MM-DD HH:MM:SS' (space separator, no T, no Z, no offset suffix).
    Safe to call on already-space-format input.
    Negative offsets (e.g. -05:00) are stripped by the final [:19] slice,
    not by split("+")[0] which only handles positive offsets.
    """
    return val.replace("T", " ").split("+")[0].rstrip("Z").split(".")[0][:19]

# COMMAND ----------


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
    Advance sysparm_offset by _PAGE_SIZE until the API returns an empty result.

    Typical sources: ServiceNow Table API  (/table/incident, /table/problem …)

    NOTE — for compound queries (e.g. active=true^category=hardware combined
    with a watermark), use the Descriptor paginator with filter_mode="sysparm"
    and a custom filter_template such as "active=true^category=hardware^{col}>={val}".
    You can also add sysparm_fields, sysparm_display_value, etc. via extra_params.

    ingestion_entity_config setup:
        pagination_type        = OffsetPagination
        endpoint_url           = /api/now/table/incident
        watermark_column_name  = sys_updated_on
        watermark_column_value = <ServiceNow datetime e.g. 2026-01-01 00:00:00>

    Yields:
        list[dict] — one page of ServiceNow records.
    """
    url    = f"{base_url}{endpoint}"
    offset = 0
    _page  = 0

    while True:
        _page += 1
        if _page >= _MAX_PAGES:
            log.warning(
                f"OffsetPagination reached _MAX_PAGES={_MAX_PAGES} safety limit — stopping. "
                "Increase _MAX_PAGES in paginators_servicenow.py if the API genuinely has this many pages."
            )
            return
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

# COMMAND ----------

PAGINATION_DISPATCH_SERVICENOW: dict = {
    "OffsetPagination": paginate_by_offset,
}




# Databricks notebook source
# MAGIC %md
# MAGIC ### _lib/paginators
# MAGIC Aggregator for all pagination handlers.  Loads the source-specific paginator
# MAGIC libraries, adds the generic Descriptor paginator, and exposes a single
# MAGIC `PAGINATION_DISPATCH` dict used by `01_Entity_Extractor`.
# MAGIC
# MAGIC **Adding a new source:**
# MAGIC 1. Create `_lib/paginators_<source>.py` following the same pattern.
# MAGIC 2. Add a `%run` line below for the new file.
# MAGIC 3. Merge its `PAGINATION_DISPATCH_<SOURCE>` into the final dict.
# MAGIC 4. Set the matching string(s) in `ingestion_entity_config.pagination_type`.
# MAGIC
# MAGIC **Source files:**
# MAGIC   - `paginators_ado.py`          ContinuationToken · OData · WIQL
# MAGIC   - `paginators_github.py`       LinkHeader · PageNumber
# MAGIC   - `paginators_servicenow.py`   OffsetPagination
# MAGIC   - `paginators.py` (this file)  Descriptor  (config-driven generic paginator)
# MAGIC
# MAGIC Depends on: `logging_utils`, `http_client` (must be `%run` first).

# COMMAND ----------
# MAGIC %run ./_lib/paginators_ado

# COMMAND ----------
# MAGIC %run ./_lib/paginators_github

# COMMAND ----------
# MAGIC %run ./_lib/paginators_servicenow

# COMMAND ----------
import json as _json
from typing import Iterator, Optional

_PAGE_SIZE = 100     # records per API request — canonical value used by Descriptor
_MAX_PAGES = 10_000  # safety cap shared by all paginators


def _to_snow_dt(val: str) -> str:
    """
    Convert an ISO-8601 datetime to 'YYYY-MM-DD HH:MM:SS' format.
    sysparm_query expects a space separator — no T, no Z, no offset suffix.
    Safe to call on already-space-format input.
    Negative offsets (e.g. -05:00) are stripped by the final [:19] slice,
    not by split("+")[0] which only handles positive offsets.
    """
    return val.replace("T", " ").split("+")[0].rstrip("Z").split(".")[0][:19]


# Maps filter_mode → (default_template, query_param_name, optional_value_transform).
# value_transform is applied to wm_val before template substitution;
# None means use wm_val as-is (correct for ISO-8601 APIs like OData / GitHub).
_FILTER_DEFAULTS: dict = {
    "odata":        ("{col} ge {val}",   "$filter",       None),
    "odata_quoted": ("{col} ge '{val}'", "$filter",       None),
    "sysparm":      ("{col}>={val}",     "sysparm_query", _to_snow_dt),
}

# COMMAND ----------


def _descriptor_params(
    desc:   dict,
    page:   int,
    offset: int,
    token:  Optional[str],
    wm_col: str,
    wm_val: str,
) -> dict:
    """Build the query-string params dict from a parsed Descriptor config."""
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
            f"Supported: querystring, {', '.join(_FILTER_DEFAULTS)}. "
            "Check ingestion_entity_config.pagination_config."
        )

    order_by = desc.get("order_by")
    if order_by:
        params["$orderby"] = order_by.format(col=wm_col)

    extra = desc.get("extra_params")
    if isinstance(extra, dict):
        params.update(extra)

    return params


def _descriptor_records(body, records_path: Optional[str]) -> list:
    """Extract the records list from a parsed response body."""
    if records_path is None:
        return body if isinstance(body, list) else []
    return body.get(records_path, []) if isinstance(body, dict) else []


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
    Config-driven generic paginator.  Handles common REST pagination patterns
    without new Python code per API — only a JSON config row in the database.

    pagination_config is stored in ingestion_entity_config.pagination_config.

    ── next_signal options ──────────────────────────────────────────────────────
    "empty_batch"         Stop when the API returns no records.
                          Requires offset_mode + offset_param to advance pages.
                          Sources: ServiceNow, any page/offset API.

    "link_header"         Follow rel="next" from the Link response header.
                          Sources: GitHub REST API.

    "odata_next_link"     Follow @odata.nextLink from the response body.
                          Sources: MS Graph, Dynamics 365, ADO Analytics.

    "continuation_header" Read x-ms-continuationtoken from the response header.
                          Sources: ADO REST (/_apis/wit/workitems …).

    ── filter_mode options ──────────────────────────────────────────────────────
    "querystring"    wm_col=wm_val as a direct query param.
    "odata"          $filter={col} ge {val}
    "odata_quoted"   $filter={col} ge '{val}'
    "sysparm"        sysparm_query={col}>={val}

    ── offset_mode options (for empty_batch signal) ─────────────────────────────
    "page"    Increment the page-number param (1 → 2 → 3 …).
    "offset"  Increment an offset param by _PAGE_SIZE (0 → 100 → 200 …).
    "none"    No offset param — only valid with continuation/nextLink signals.

    ── extra_params ─────────────────────────────────────────────────────────────
    Optional dict of fixed params merged into every request after all others.
    Use for: sysparm_fields, $select, $expand, api-version, etc.

    ── Example configs ──────────────────────────────────────────────────────────
    GitHub Issues (link_header):
        {"page_size_param":"per_page","offset_mode":"none","records_path":null,
         "next_signal":"link_header","filter_mode":"querystring"}

    ServiceNow compound query:
        {"page_size_param":"sysparm_limit","offset_param":"sysparm_offset",
         "offset_mode":"offset","records_path":"result","next_signal":"empty_batch",
         "filter_mode":"sysparm","filter_template":"active=true^{col}>={val}",
         "extra_params":{"sysparm_fields":"sys_id,number,short_description,state,sys_updated_on"}}

    ADO Analytics (OData):
        {"page_size_param":"$top","offset_mode":"none","records_path":"value",
         "next_signal":"odata_next_link","filter_mode":"odata","order_by":"{col} asc"}

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
            "offset_mode='page' or offset_mode='offset'. "
            "With offset_mode='none', every request is identical and the generator "
            "loops forever. Use 'link_header' or 'odata_next_link' for cursor-based "
            "APIs, or set offset_mode='page'/'offset' for page/offset APIs."
        )

    if desc.get("filter_mode", "querystring") == "querystring" and desc.get("filter_template"):
        raise ValueError(
            "Descriptor config error: filter_template is unused when filter_mode='querystring'. "
            "Remove filter_template, or change filter_mode to 'odata', 'odata_quoted', or 'sysparm'."
        )

    url      = f"{base_url}{endpoint}"
    next_url: Optional[str] = None
    page     = 1
    offset   = 0
    token: Optional[str] = None
    page_num = 0

    while True:
        if next_url:
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

        if page_num >= _MAX_PAGES:
            log.warning(
                f"Descriptor reached _MAX_PAGES={_MAX_PAGES} safety limit — stopping. "
                "Increase _MAX_PAGES in paginators.py if the API genuinely has this many pages."
            )
            return

        if signal == "link_header":
            # _parse_link_next is injected into scope by %run ./_lib/paginators_github
            _has_next = _parse_link_next(resp.headers.get("Link", ""))
        elif signal == "odata_next_link":
            _has_next = body.get("@odata.nextLink") if isinstance(body, dict) else None
        elif signal == "continuation_header":
            _has_next = resp.headers.get("x-ms-continuationtoken")
        else:
            _has_next = None

        log.debug(
            f"Descriptor  page={page_num}"
            f"  signal={signal}"
            f"  batch_size={len(batch)}"
            f"  has_next={bool(_has_next)}"
        )

        if not batch:
            return

        yield batch

        if signal == "empty_batch":
            offset_mode = desc.get("offset_mode", "none")
            if offset_mode == "page":
                page += 1
            elif offset_mode == "offset":
                offset += _PAGE_SIZE

        elif signal == "link_header":
            next_url = _has_next
            if not next_url:
                return

        elif signal == "odata_next_link":
            next_url = _has_next
            if not next_url:
                return

        elif signal == "continuation_header":
            token = _has_next
            if not token:
                return

        else:
            log.warning(f"Descriptor: unknown next_signal='{signal}' — stopping")
            return

# COMMAND ----------
# ── Merged dispatch map ────────────────────────────────────────────────────────
# PAGINATION_DISPATCH_ADO, _GITHUB, _SERVICENOW are injected by the %run cells above.
# Descriptor lives here because it is source-agnostic.

PAGINATION_DISPATCH: dict = {
    **PAGINATION_DISPATCH_ADO,
    **PAGINATION_DISPATCH_GITHUB,
    **PAGINATION_DISPATCH_SERVICENOW,
    "Descriptor": paginate_by_descriptor,
}




-- ============================================================
-- Ingestion Framework — Sample INSERT scripts
-- Replace placeholder values:
--   {ado_org}        Azure DevOps organisation name
--   {ado_project}    Azure DevOps project name
--   {gh_owner}       GitHub organisation or user
--   {snow_instance}  ServiceNow instance subdomain (e.g. mycompany)
-- Auth secrets must be stored in Databricks secret scope "ingestion-secrets"
-- per the key convention in _lib/auth.py
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- ingestion_source_config  (4 rows — one per base URL)
-- ADO REST and ADO Analytics live on different hosts, so they
-- need separate source rows even though both use ADO_OAUTH.
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_source_config
    (source_id, source_name,              base_url,                                                 auth_type,   is_active)
VALUES
    (1,  'Azure DevOps REST',             'https://dev.azure.com/{ado_org}/{ado_project}',           'ADO_OAUTH', TRUE),
    (2,  'Azure DevOps Analytics',        'https://analytics.dev.azure.com/{ado_org}/{ado_project}', 'ADO_OAUTH', TRUE),
    (3,  'GitHub',                        'https://api.github.com',                                  'GITHUB_APP', TRUE),
    (4,  'ServiceNow',                    'https://{snow_instance}.service-now.com',                  'SNOW_OAUTH', TRUE);


-- ────────────────────────────────────────────────────────────
-- ADO REST  (source_id = 1)  — 5 entities via REST paginators
-- Secret prefix: "azure-devops-rest"
--   azure-devops-rest-tenant-id
--   azure-devops-rest-client-id
--   azure-devops-rest-client-secret
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                   pagination_type,    pagination_config,
     landing_zone_path,                                  watermark_column_name,  watermark_column_value, active_flag)
VALUES
-- 1. Epics — WIQL two-step with explicit field list (faster than $expand=all)
(   'ADO_EPICS',             1,         '/_apis/wit/workitems',          'WIQL',
    '{"wiql_where":"[System.WorkItemType] = ''Epic''","fields":["System.Id","System.Title","System.WorkItemType","System.State","System.AreaPath","System.IterationPath","System.AssignedTo","System.ChangedDate","Microsoft.VSTS.Common.Priority"]}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/epics',          'System.ChangedDate', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 2. Pull Requests — Descriptor continuation_header
(   'ADO_PULL_REQUESTS',     1,         '/_apis/git/pullrequests',       'Descriptor',
    '{"page_size_param":"$top","offset_mode":"none","records_path":"value","next_signal":"continuation_header","filter_mode":"odata_quoted","extra_params":{"searchCriteria.status":"all","api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/pull_requests',  'searchCriteria.minTime', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 3. Builds — Descriptor continuation_header
(   'ADO_BUILDS',            1,         '/_apis/build/builds',           'Descriptor',
    '{"page_size_param":"$top","offset_mode":"none","records_path":"value","next_signal":"continuation_header","filter_mode":"querystring","extra_params":{"statusFilter":"all","queryOrder":"startTimeAscending","api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/builds',         'minTime', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 4. Releases — Descriptor continuation_header
(   'ADO_RELEASES',          1,         '/_apis/release/releases',       'Descriptor',
    '{"page_size_param":"$top","offset_mode":"none","records_path":"value","next_signal":"continuation_header","filter_mode":"querystring","extra_params":{"$expand":"approvals,artifacts","queryOrder":"ascending","api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/releases',       'minCreatedTime', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 5. Git Repositories — full load (no watermark filter), Descriptor empty_batch + offset
(   'ADO_REPOSITORIES',      1,         '/_apis/git/repositories',       'Descriptor',
    '{"page_size_param":"$top","offset_param":"$skip","offset_mode":"offset","records_path":"value","next_signal":"empty_batch","filter_mode":"querystring","extra_params":{"api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/repositories',   'lastUpdateTime', TIMESTAMP '2000-01-01T00:00:00Z', TRUE);


-- ── Additional WIQL examples (add to source_id=1 as needed) ─────────────
-- Features:
--   pagination_config = '{"wiql_where":"[System.WorkItemType] = ''Feature''","fields":["System.Id","System.Title","System.State","System.AreaPath","System.ChangedDate","System.Parent"]}'
--
-- Bugs and User Stories (multi-type in one entity):
--   pagination_config = '{"wiql_where":"[System.WorkItemType] IN (''Bug'',''User Story'') AND [System.TeamProject] = ''@project''","fields":["System.Id","System.Title","System.WorkItemType","System.State","System.AssignedTo","System.ChangedDate"]}'
--
-- All types in an area path:
--   pagination_config = '{"wiql_where":"[System.AreaPath] UNDER ''MyProject\\MyTeam''","fields":["System.Id","System.Title","System.WorkItemType","System.State","System.ChangedDate"]}'

-- ────────────────────────────────────────────────────────────
-- ADO Analytics  (source_id = 2)  — 5 entities via OData
-- Secret prefix: "azure-devops-analytics"
-- Same Azure AD app registration can be reused — add a second
-- set of secrets under the "azure-devops-analytics" prefix or
-- share them by naming source_name identically and using one
-- source row (then source_id 1 and 2 collapse to one row).
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                        pagination_type, pagination_config,
     landing_zone_path,                                        watermark_column_name, watermark_column_value, active_flag)
VALUES
-- 6. Work Items — OData
(   'ADO_WORKITEMS_ODATA',   2,         '/_odata/v4.0/WorkItems',            'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/workitems_odata',   'ChangedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 7. Work Item Revisions — OData (full history)
-- NOTE: watermark must be ChangedDate (when the revision was created), NOT RevisedDate.
-- RevisedDate = 9999-12-31 for every current revision — watermarking on it re-fetches
-- all current-state records on every run regardless of the watermark value.
(   'ADO_WI_REVISIONS',      2,         '/_odata/v4.0/WorkItemRevisions',    'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/wi_revisions',      'ChangedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 8. Pipeline Runs — OData
(   'ADO_PIPELINE_RUNS',     2,         '/_odata/v4.0/PipelineRuns',         'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/pipeline_runs',     'CreatedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 9. Test Results Daily — OData (aggregated by day)
-- NOTE: watermark must be Date (a proper datetime), NOT DateSK.
-- DateSK is an integer surrogate key (20200101, 20200102 …); filtering
-- DateSK ge '2020-01-01T00:00:00Z' produces a type-mismatch 400 from Analytics.
(   'ADO_TEST_RESULTS',      2,         '/_odata/v4.0/TestResultsDaily',     'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/test_results_daily','Date',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 10. Test Runs — OData
(   'ADO_TEST_RUNS',         2,         '/_odata/v4.0/TestRuns',             'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/test_runs',         'CreatedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE);


-- ────────────────────────────────────────────────────────────
-- GitHub  (source_id = 3)  — 10 entities via LinkHeader
-- Secret prefix: "github"
--   github-app-id
--   github-installation-id
--   github-private-key   (full PEM, newlines preserved in secret)
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                                        pagination_type, pagination_config,
     landing_zone_path,                                              watermark_column_name, watermark_column_value, active_flag)
VALUES
-- 11. Issues (all repos in org — repeat per repo or use GitHub GraphQL for cross-repo)
(   'GH_ISSUES',             3,         '/repos/{gh_owner}/{repo}/issues',   'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/issues',          'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 12. Pull Requests
(   'GH_PULL_REQUESTS',      3,         '/repos/{gh_owner}/{repo}/pulls',    'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/pull_requests',   'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 13. Commits
(   'GH_COMMITS',            3,         '/repos/{gh_owner}/{repo}/commits',  'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/commits',         'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 14. Workflow Runs (GitHub Actions)
(   'GH_WORKFLOW_RUNS',      3,         '/repos/{gh_owner}/{repo}/actions/runs', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":"workflow_runs","next_signal":"empty_batch","filter_mode":"querystring","extra_params":{"status":"completed"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/workflow_runs',   'created',       TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 15. Releases
(   'GH_RELEASES',           3,         '/repos/{gh_owner}/{repo}/releases', 'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/releases',        'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 16. Code Scanning Alerts
(   'GH_CODE_SCANNING',      3,         '/repos/{gh_owner}/{repo}/code-scanning/alerts', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":null,"next_signal":"link_header","filter_mode":"querystring","extra_params":{"sort":"updated","direction":"asc"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/code_scanning',   'updated_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 17. Secret Scanning Alerts
(   'GH_SECRET_SCANNING',    3,         '/repos/{gh_owner}/{repo}/secret-scanning/alerts', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":null,"next_signal":"link_header","filter_mode":"querystring","extra_params":{"sort":"updated","direction":"asc"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/secret_scanning', 'created_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 18. Dependabot Alerts
(   'GH_DEPENDABOT',         3,         '/repos/{gh_owner}/{repo}/dependabot/alerts', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":null,"next_signal":"link_header","filter_mode":"querystring","extra_params":{"sort":"updated","direction":"asc"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/dependabot',      'updated_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 19. Issue Comments (search API — nested under "items")
(   'GH_ISSUE_COMMENTS',     3,         '/repos/{gh_owner}/{repo}/issues/comments', 'LinkHeader', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/issue_comments',  'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 20. Check Runs (via workflow run sub-resource)
(   'GH_CHECK_RUNS',         3,         '/repos/{gh_owner}/{repo}/check-runs',     'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":"check_runs","next_signal":"empty_batch","filter_mode":"querystring","extra_params":{}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/check_runs',      'started_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE);


-- ────────────────────────────────────────────────────────────
-- ServiceNow  (source_id = 4)  — 10 entities
-- Secret prefix: "servicenow"
--   servicenow-client-id
--   servicenow-client-secret
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                        pagination_type,   pagination_config,
     landing_zone_path,                                          watermark_column_name, watermark_column_value, active_flag)
VALUES
-- 21. Incidents
(   'SNOW_INCIDENTS',        4,         '/api/now/table/incident',           'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/incidents',   'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 22. Problems
(   'SNOW_PROBLEMS',         4,         '/api/now/table/problem',            'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/problems',    'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 23. Change Requests
(   'SNOW_CHANGES',          4,         '/api/now/table/change_request',     'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/changes',     'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 24. Change Tasks
(   'SNOW_CHANGE_TASKS',     4,         '/api/now/table/change_task',        'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/change_tasks','sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 25. CMDB Config Items — Descriptor with compound query and field selection
(   'SNOW_CMDB_CI',          4,         '/api/now/table/cmdb_ci',            'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"install_status!=7^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_exclude_reference_link":"true","sysparm_fields":"sys_id,name,sys_class_name,install_status,sys_updated_on"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/cmdb_ci',     'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 26. Users (sys_user)
(   'SNOW_USERS',            4,         '/api/now/table/sys_user',           'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"active=true^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_exclude_reference_link":"true","sysparm_fields":"sys_id,user_name,first_name,last_name,email,department,sys_updated_on"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/users',       'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 27. Service Requests (sc_request)
(   'SNOW_SC_REQUESTS',      4,         '/api/now/table/sc_request',         'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/sc_requests', 'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 28. Request Items (sc_req_item)
(   'SNOW_SC_REQ_ITEMS',     4,         '/api/now/table/sc_req_item',        'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/sc_req_items','sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 29. Tasks (task — base table; use a subclass like sc_task or sn_si_task if needed)
(   'SNOW_TASKS',            4,         '/api/now/table/task',               'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"active=true^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_exclude_reference_link":"true"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/tasks',       'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 30. User Groups (sys_user_group)
(   'SNOW_USER_GROUPS',      4,         '/api/now/table/sys_user_group',     'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"active=true^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_fields":"sys_id,name,manager,type,sys_updated_on"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/user_groups', 'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE);


-- ────────────────────────────────────────────────────────────
-- Verification queries
-- ────────────────────────────────────────────────────────────
-- SELECT s.source_name, e.entity_id, e.pagination_type, e.watermark_column_name
-- FROM   ingestion_entity_config e
-- JOIN   ingestion_source_config s ON e.source_id = s.source_id
-- WHERE  e.active_flag = TRUE
-- ORDER  BY s.source_id, e.entity_id;






# COMMAND ----------

# DBTITLE 1,workflow
{
  "_instructions": [
    "Replace every value that begins with REPLACE: before importing into Databricks.",
    "Import via: Workflows UI → Create job → ... → Import JSON,",
    "  or:  databricks jobs create --json @workflow_definition.json",
    "One workflow per source_id.  Set job parameter source_id to the desired source.",
    "Tune concurrency (semaphore): 4 is safe for ADO/SNOW; raise to 8 for GitHub.",
    "  Higher values risk 429 rate-limits; lower values reduce parallelism.",
    "existing_cluster_id: use an all-purpose cluster that already has the",
    "  ingestion_entity_config and ingestion_source_config tables attached.",
    "  Alternatively replace with a new_cluster block for ephemeral job clusters."
  ],

  "name": "ingestion-batch",

  "parameters": [
    { "name": "source_id", "default": "1",    "description": "ingestion_source_config.source_id to ingest" },
    { "name": "log_level", "default": "INFO",  "description": "DEBUG | INFO | WARNING | ERROR" }
  ],

  "tasks": [

    {
      "task_key": "prepare_batch",
      "description": "Query active entities for the source; return batch_run_id + entity list for the For Each task.",
      "notebook_task": {
        "notebook_path": "REPLACE: /Workspace/Shared/ingestion-framework/00_Orchestrator_Ingestion",
        "base_parameters": {
          "source_id": "{{job.parameters.source_id}}",
          "log_level": "{{job.parameters.log_level}}"
        }
      },
      "existing_cluster_id": "REPLACE: your-all-purpose-cluster-id",
      "timeout_seconds": 300,
      "max_retries": 1,
      "min_retry_interval_millis": 15000
    },

    {
      "task_key": "extract_entities",
      "description": "For Each entity_id: run 01_Entity_Extractor. concurrency is the semaphore — max parallel extractions at any one time.",
      "depends_on": [{ "task_key": "prepare_batch" }],
      "for_each_task": {
        "inputs": "{{tasks.prepare_batch.values.entities}}",
        "concurrency": 4,
        "task": {
          "task_key": "extract_single_entity",
          "notebook_task": {
            "notebook_path": "REPLACE: /Workspace/Shared/ingestion-framework/01_Entity_Extractor",
            "base_parameters": {
              "config_json":  "{{input}}",
              "run_id":       "{{task.run_id}}",
              "batch_run_id": "{{job.run_id}}",
              "log_level":    "{{job.parameters.log_level}}"
            }
          },
          "existing_cluster_id": "REPLACE: your-all-purpose-cluster-id",
          "timeout_seconds": 3600,
          "max_retries": 1,
          "min_retry_interval_millis": 60000
        }
      }
    }

  ],

  "max_concurrent_runs": 1,
  "timeout_seconds": 18000,

  "email_notifications": {
    "on_failure": ["REPLACE: oncall@yourcompany.com"],
    "no_alert_for_skipped_runs": true
  },

  "health": {
    "rules": [
      {
        "metric": "RUN_DURATION_SECONDS",
        "op":     "GREATER_THAN",
        "value":  14400
      }
    ]
  }
}


# COMMAND ----------

# DBTITLE 1,00_Orchestrator_ingestion
# Databricks notebook source
# MAGIC %md
# MAGIC ## 00 · Batch Orchestrator — Enterprise Ingestion Framework
# MAGIC Prepare step for a Databricks Workflow **For Each** batch.
# MAGIC
# MAGIC Queries all active entities for the given `source_id`, prefetches the OAuth
# MAGIC token once, encrypts it with AES-256-GCM, and writes it to
# MAGIC catalog.ingestion.token_cache (shared Delta table), then exits with a JSON payload
# MAGIC consumed by the downstream For Each task:
# MAGIC
# MAGIC ```json
# MAGIC {"entities": [{"entity_id": "ADO_EPICS", "source_id": 1, ...}, ...]}
# MAGIC ```
# MAGIC
# MAGIC The For Each task iterates over `entities` with `concurrency` as the semaphore.
# MAGIC Each entity extractor reads the encrypted token from catalog.ingestion.token_cache (L2)
# MAGIC instead of making its own token request — 1 token POST per batch, not per entity.
# MAGIC No credentials or tokens pass through workflow parameters or task output values.
# MAGIC
# MAGIC **Widget parameters:**
# MAGIC   `source_id`  INT    — ingestion_source_config.source_id
# MAGIC   `log_level`  STRING — DEBUG / INFO / WARNING / ERROR  (default INFO)

# COMMAND ----------
# MAGIC %run ./_lib/logging_utils

# COMMAND ----------
# MAGIC %run ./_lib/auth

# COMMAND ----------
import json
from datetime import datetime, timezone

# COMMAND ----------
# ── WIDGETS ────────────────────────────────────────────────────────────────────
dbutils.widgets.text(    "source_id", "",     "Source ID  (INT)  e.g. 1")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"])

# COMMAND ----------
# ── SETUP ──────────────────────────────────────────────────────────────────────
_raw_source_id = dbutils.widgets.get("source_id").strip()
log_level      = dbutils.widgets.get("log_level")

try:
    source_id = int(_raw_source_id)
except (ValueError, TypeError):
    raise ValueError(
        f"source_id widget must be an integer matching "
        f"ingestion_source_config.source_id, got '{_raw_source_id}'"
    )
if source_id <= 0:
    raise ValueError(f"source_id must be a positive integer, got {source_id}")

batch_start = datetime.now(timezone.utc)

log = ContextLogger(
    _setup_logger("ingestion.orchestrator", log_level),
    {"source_id": source_id},
)

log.info("Batch orchestrator started")

# COMMAND ----------
# ── QUERY ACTIVE ENTITIES ──────────────────────────────────────────────────────
config_rows = spark.sql(f"""
    SELECT
        e.entity_id,
        e.source_id,
        e.endpoint_url,
        e.pagination_type,
        e.pagination_config,
        e.landing_zone_path,
        e.watermark_column_name,
        CAST(e.watermark_column_value AS STRING) AS watermark_column_value,
        s.source_name,
        s.base_url,
        s.auth_type
    FROM   ingestion_entity_config   e
    INNER JOIN ingestion_source_config s ON e.source_id = s.source_id
    WHERE  e.source_id   = {source_id}
      AND  e.active_flag = TRUE
      AND  s.is_active   = TRUE
    ORDER  BY e.entity_id
""").collect()

if not config_rows:
    raise ValueError(
        f"No active entities found for source_id={source_id}. "
        "Check ingestion_entity_config.active_flag and ingestion_source_config.is_active."
    )

source_name = config_rows[0]["source_name"]

entities = [
    {
        "entity_id":             row["entity_id"],
        "source_id":             row["source_id"],
        "endpoint_url":          row["endpoint_url"],
        "pagination_type":       row["pagination_type"],
        "pagination_config":     row["pagination_config"],
        "landing_zone_path":     row["landing_zone_path"],
        "watermark_column_name": row["watermark_column_name"],
        "watermark_column_value": row["watermark_column_value"],
        "source_name":           row["source_name"],
        "base_url":              row["base_url"],
        "auth_type":             row["auth_type"],
    }
    for row in config_rows
]

log.info(
    f"Batch prepared"
    f"  source={source_name}"
    f"  entity_count={len(entities)}"
    f"  entities={[e['entity_id'] for e in entities]}"
)

# COMMAND ----------
# ── TOKEN PREFETCH ─────────────────────────────────────────────────────────────
# Fetch the OAuth token ONCE per batch run, encrypt it with AES-256-GCM, and
# write it to catalog.ingestion.token_cache — 1 token POST per batch, not 1
# per entity.  This avoids hammering the IdP and keeps token latency off the
# per-entity hot path.
#
# Token cache layers (entity extractor side):
#   L1  in-memory headers dict  — valid for one entity run; updated in-place on 401
#   L2  token_cache Delta table — shared across all entities for the same source;
#                                  seeded here; updated by whichever entity first
#                                  hits a 401 and refreshes mid-run
#   L3  live token endpoint     — fallback when L2 misses or decrypt fails
#
# Auth types that use prefetch (short-lived OAuth tokens):
#   ADO_OAUTH, SNOW_OAUTH, GITHUB_APP  →  written to DB here
#
# Auth types that skip prefetch (long-lived credentials):
#   PAT, GITHUB_PAT, TOKEN, OAUTH2, SNOW_BASIC  →  entity extractors read
#   directly from Databricks Secrets (L3 only; no DB write needed).
#
# Non-fatal:
#   Any failure here means the DB row is absent — entity extractors fall back
#   to L3 (live token fetch) automatically.  The batch continues.
# ──────────────────────────────────────────────────────────────────────────────

_OAUTH_PREFETCH_TYPES = {"ADO_OAUTH", "SNOW_OAUTH", "GITHUB_APP"}
_enc_token            = ""

if config_rows[0]["auth_type"].upper() in _OAUTH_PREFETCH_TYPES:
    try:
        # Legacy guard — _PREFETCH_IN_PROGRESS is checked inside _read_prefetched_token()
        # which is not called in production.  add / discard below have no effect;
        # retained for symmetry with the unused legacy functions in auth.py.
        _key_prefix = source_name.lower().replace(" ", "-")
        _PREFETCH_IN_PROGRESS.add(_key_prefix)
        try:
            _hdrs = get_auth_headers(
                config_rows[0]["auth_type"],
                source_name,
                log,
                config_rows[0]["base_url"],
            )
        finally:
            _PREFETCH_IN_PROGRESS.discard(_key_prefix)

        # Strip "Bearer " prefix — encrypt_token stores the raw token value only.
        # build_auth_headers_from_token() in auth.py re-adds the correct scheme
        # when each entity extractor reconstructs its Authorization header.
        _raw_token = _hdrs["Authorization"].split(" ", 1)[1]
        _enc_token = encrypt_token(_raw_token, log)
        if _enc_token:
            write_token_cache(source_name, _enc_token, log)
            log.info(
                f"Token prefetched and encrypted  "
                f"auth_type={config_rows[0]['auth_type']}  "
                f"entity_count={len(entities)}"
            )
    except Exception as _prefetch_exc:
        log.warning(
            f"Token prefetch failed — entity extractors will fetch their own tokens  "
            f"error={_prefetch_exc}"
        )

# COMMAND ----------
# ── EXIT — return payload for the downstream For Each task ─────────────────────
# The Workflow references this value as:
#   {{tasks.prepare_batch.values.entities}}   → JSON array of entity config dicts
#
# Raw tokens never appear in the exit payload.  Each entity extractor reads its
# token from catalog.ingestion.token_cache (L2) at startup.
dbutils.notebook.exit(json.dumps({
    "entities": entities,
}))


# COMMAND ----------

# DBTITLE 1,01_Entity_Extractor
# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · Entity Extractor — Enterprise Ingestion Framework
# MAGIC Single-entity extraction driver.  Accepts `source_id` (INT) and
# MAGIC `entity_id` (STRING) as explicit widget parameters, loads the combined
# MAGIC config row, runs the full extract → land → audit → watermark cycle.
# MAGIC
# MAGIC **Invoked by:** Databricks Workflow (one workflow per entity) or run
# MAGIC interactively by setting both widgets.
# MAGIC
# MAGIC Widget parameters (match ingestion_entity_config / ingestion_source_config DDL):
# MAGIC   source_id  INT    — ingestion_source_config.source_id
# MAGIC   entity_id  STRING — ingestion_entity_config.entity_id
# MAGIC   run_id     STRING — passed as {{job.run_id}} by the workflow; auto-generated UUID when empty (interactive)

# COMMAND ----------
# MAGIC %run ./_lib/logging_utils

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
# MAGIC # Swap the line above for ./_lib/landing_writer_spark when landing_zone_path
# MAGIC # is an Azure ADLS Gen2 URI (abfss://…).  Both files export write_pages_to_json
# MAGIC # with the same signature — no other change needed.

# COMMAND ----------
# ── IMPORTS ────────────────────────────────────────────────────────────────────
import json
import uuid
from datetime import datetime, timezone


def _fmt_wm(val) -> str:
    """
    Format a watermark value as ISO-8601 UTC for API filter parameters.
    PySpark TIMESTAMP columns arrive as naive datetime objects; str() on them
    produces 'YYYY-MM-DD HH:MM:SS' (space format) which GitHub 'since',
    ADO OData $filter, WIQL, and ADO REST $filter all reject.
    If watermark_column_value is stored as a STRING column (not TIMESTAMP),
    this normalises the space-format value to ISO-8601 automatically.
    """
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(val)
    # Normalise space-format string  '2026-01-01 10:00:00'  →  '2026-01-01T10:00:00Z'
    if len(s) >= 19 and s[10] == " ":
        return s[:10] + "T" + s[11:19] + "Z"
    return s

# COMMAND ----------
# ── WIDGETS ────────────────────────────────────────────────────────────────────
dbutils.widgets.text(    "source_id",    "",     "Source ID  (INT)  e.g. 5")
dbutils.widgets.text(    "entity_id",    "",     "Entity ID  (STRING)  e.g. SNOW_INCIDENTS")
dbutils.widgets.text(    "run_id",       "",     "Run ID  (set by {{job.run_id}}; UUID auto-generated when empty)")
dbutils.widgets.text(    "batch_run_id", "",     "Batch Run ID  (set by 00_Orchestrator For Each task; empty in standalone runs)")
dbutils.widgets.text(    "config_json",  "",     "Full config JSON  (set by 00_Orchestrator; leave empty to load from DB in standalone runs)")
dbutils.widgets.dropdown("log_level",   "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"])

# COMMAND ----------
# ── PARAMETERS & EARLY SETUP ──────────────────────────────────────────────────
# RUN_ID and the base logger are created first so that validation failures
# can be written to ingestion_audit_log before the notebook exits.

_raw_source_id  = dbutils.widgets.get("source_id").strip()
entity_id       = dbutils.widgets.get("entity_id").strip()
_wgt_run_id     = dbutils.widgets.get("run_id").strip()
batch_run_id    = dbutils.widgets.get("batch_run_id").strip()
config_json_raw = dbutils.widgets.get("config_json").strip()
log_level       = dbutils.widgets.get("log_level")

# Pre-initialise so the ContextLogger line after the validation block never
# hits NameError if the validation except block's raise somehow doesn't propagate.
source_id = -1

# Use the workflow-supplied run_id when available; fall back to a UUID for
# interactive / ad-hoc runs so the audit log is always populated.
RUN_ID  = _wgt_run_id if _wgt_run_id else str(uuid.uuid4())

try:
    NB_NAME = (
        dbutils.notebook.entry_point
        .getDbutils().notebook().getContext()
        .notebookPath().get()
    )
except Exception:
    NB_NAME = "unknown"

_base_log = _setup_logger("ingestion.extractor", log_level)

# COMMAND ----------
# ── WIDGET VALIDATION ─────────────────────────────────────────────────────────
# Runs before config is loaded.  Any failure here writes a minimal FAILED audit
# row so the broken run is visible in ingestion_audit_log alongside healthy runs.

try:
    if config_json_raw:
        _pre           = json.loads(config_json_raw)
        _raw_source_id = str(_pre["source_id"])
        entity_id      = _pre["entity_id"]

    if not entity_id:
        raise ValueError("entity_id widget must not be empty")

    if not all(c.isalnum() or c == "_" for c in entity_id):
        raise ValueError(
            f"entity_id must contain only letters, digits and underscores, "
            f"got '{entity_id}'"
        )

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
    # cfg is not available yet — build a minimal stub for the audit helper.
    _run_start = datetime.now(timezone.utc)

    class _MinCfg:
        try:
            source_id = int(_raw_source_id)
        except (ValueError, TypeError):
            source_id = -1
        entity_id = entity_id or "UNKNOWN"

    try:
        upsert_audit(
            spark, RUN_ID,
            build_audit_payload(
                run_id    = RUN_ID,
                cfg       = _MinCfg(),
                nb_name   = NB_NAME,
                run_start = _run_start,
                run_end   = _run_start,
                status    = "FAILED",
                remarks   = f"Widget validation failed: {_val_exc}",
            ),
            _base_log,
        )
    except Exception as _audit_exc:
        _base_log.warning(f"Audit write failed during widget validation failure: {_audit_exc}")
    raise

log = ContextLogger(_base_log, {
    "batch_run_id": batch_run_id,   # empty string suppressed by ContextLogger when falsy
    "run_id":       RUN_ID,
    "source_id":    source_id,
    "entity_id":    entity_id,
})

log.info("Entity extractor started")

# COMMAND ----------
# ── CONFIG LOAD ───────────────────────────────────────────────────────────────
# Filter on BOTH source_id (INT, unquoted) and entity_id (STRING, quoted).

try:
    if config_json_raw:
        from types import SimpleNamespace
        cfg = SimpleNamespace(**json.loads(config_json_raw))
    else:
        _cfg_rows = spark.sql(f"""
            SELECT
                e.entity_id,
                e.endpoint_url,
                e.pagination_type,
                e.pagination_config,
                e.landing_zone_path,
                e.watermark_column_name,
                e.watermark_column_value,
                s.source_id,
                s.source_name,
                s.base_url,
                s.auth_type
            FROM ingestion_entity_config   e
            INNER JOIN ingestion_source_config s
                   ON  e.source_id = s.source_id
            WHERE  e.entity_id   = '{entity_id}'
              AND  e.source_id   = {source_id}
              AND  e.active_flag = TRUE
              AND  s.is_active   = TRUE
        """).limit(2).collect()

        if not _cfg_rows:
            raise ValueError(
                f"No active config for source_id={source_id}, entity_id='{entity_id}'. "
                "Check ingestion_entity_config.active_flag and ingestion_source_config.is_active."
            )
        if len(_cfg_rows) > 1:
            raise ValueError(
                f"Duplicate active configs for source_id={source_id}, entity_id='{entity_id}'. "
                "Ensure only one row has active_flag=TRUE in ingestion_entity_config "
                "and is_active=TRUE in ingestion_source_config."
            )
        cfg = _cfg_rows[0]

except Exception as _cfg_exc:
    _cfg_run_start = datetime.now(timezone.utc)

    class _CfgStub:
        source_id = source_id
        entity_id = entity_id

    try:
        upsert_audit(
            spark, RUN_ID,
            build_audit_payload(
                run_id    = RUN_ID,
                cfg       = _CfgStub(),
                nb_name   = NB_NAME,
                run_start = _cfg_run_start,
                run_end   = _cfg_run_start,
                status    = "FAILED",
                remarks   = f"Config load failed: {_cfg_exc}",
            ),
            log,
        )
    except Exception as _audit_exc:
        log.warning(f"Audit write failed during config load failure: {_audit_exc}")
    raise

log.info(
    f"Config loaded"
    f"  source={cfg.source_name}"
    f"  auth={cfg.auth_type}"
    f"  pagination={cfg.pagination_type}"
    f"  watermark_col={cfg.watermark_column_name}"
    f"  watermark_val={cfg.watermark_column_value}"
)

# COMMAND ----------
# ── OPEN AUDIT ROW (RUNNING) ──────────────────────────────────────────────────
# Written before any network call so that failures in auth or HTTP setup
# (secret missing, connection refused) also close as FAILED in the audit table.
# Any run killed mid-flight (OOM, cluster eviction, timeout) leaves a visible
# RUNNING row detectable with:
#
#   SELECT * FROM ingestion_audit_log
#   WHERE  status = 'RUNNING'
#     AND  run_start_time < current_timestamp() - INTERVAL 2 HOURS

run_start = datetime.now(timezone.utc)

try:
    upsert_audit(
        spark, RUN_ID,
        build_audit_payload(
            run_id    = RUN_ID,
            cfg       = cfg,
            nb_name   = NB_NAME,
            run_start = run_start,
            status    = "RUNNING",
            remarks   = "Extraction started",
        ),
        log,
    )
    log.info("Audit row opened  status=RUNNING")
except Exception as _running_exc:
    # Non-fatal — a missing RUNNING row is a monitoring gap, not a data gap.
    # The main try block will still write the final SUCCESS/FAILED row.
    log.warning(f"RUNNING audit write failed — continuing  error={_running_exc}")

# COMMAND ----------
# ── EXTRACT → LAND → AUDIT → WATERMARK ───────────────────────────────────────
#
# Module responsibilities:
#   paginators.py      yields list[dict] one page at a time
#   landing_writer.py  buffers pages in fixed-size chunks; each chunk is one Parquet
#                      append (most incremental runs fit in one chunk = one file)
#   audit.py           MERGE-based upsert keeps every close idempotent
#
# Landing path layout:
#   <landing_zone_path>/extracted_at=<YYYYMMDDTHHMMSSZ>/<RUN_ID>/
#   RUN_ID sub-folder guarantees uniqueness per run so append mode is safe
#   on retries.
#
# Counter variables pre-initialised so the except block never hits NameError
# if an exception fires before the try block assigns them.

records_read   = 0
records_loaded = 0
landing_path   = ""

try:
    # ── CONFIG VALIDATION ─────────────────────────────────────────────────────
    # Inside the try block so a bad pagination_type or missing field also
    # closes the RUNNING audit row as FAILED rather than leaving it open.
    _REQUIRED = [
        "entity_id", "endpoint_url", "pagination_type",
        "landing_zone_path", "watermark_column_name", "watermark_column_value",
        "source_name", "base_url", "auth_type",
    ]
    missing = [f for f in _REQUIRED if not getattr(cfg, f, None)]
    if missing:
        raise ValueError(f"Config row is missing required fields: {missing}")

    if cfg.pagination_type not in PAGINATION_DISPATCH:
        raise ValueError(
            f"pagination_type='{cfg.pagination_type}' is not registered. "
            f"Valid values: {sorted(PAGINATION_DISPATCH)}"
        )

    if cfg.pagination_type == "Descriptor" and not getattr(cfg, "pagination_config", None):
        raise ValueError(
            "pagination_type='Descriptor' requires a non-NULL pagination_config. "
            "Add a JSON descriptor to ingestion_entity_config.pagination_config."
        )

    log.info("Config validation passed")

    # ── INFRASTRUCTURE SETUP ──────────────────────────────────────────────────
    # Placed inside the try block so auth failures (secret missing) and HTTP
    # session errors are caught and written to the audit table as FAILED.
    http_session = build_http_session()
    log.debug("HTTP session built")

    # ── TOKEN LOAD ────────────────────────────────────────────────────────────
    # Token resolution order for expiring OAuth types
    # (ADO_OAUTH, SNOW_OAUTH, GITHUB_APP):
    #   L1  in-memory auth_headers — valid for this entity run; updated in-place
    #       by safe_get/safe_post on any 401 (via token_refresher below)
    #   L2  catalog.ingestion.token_cache — shared Delta table seeded by the
    #       orchestrator; updated by whichever entity first hits a 401
    #   L3  live token endpoint — fetched from Databricks Secrets + auth provider
    #
    # Non-OAuth types (PAT, GITHUB_PAT, TOKEN, OAUTH2, SNOW_BASIC) skip L2
    # — their credentials are long-lived and live in Databricks Secrets (L3 only).
    #
    # _my_token_ts records the DB fetched_at of the token this entity is using.
    # token_refresher() compares it against the current DB fetched_at: if the DB
    # is newer, another entity already refreshed — use that token, skip L3.

    _OAUTH_TYPES = {"ADO_OAUTH", "SNOW_OAUTH", "GITHUB_APP"}
    _is_oauth    = cfg.auth_type.upper() in _OAUTH_TYPES
    _my_token_ts = run_start   # default: treat token as loaded at batch start

    if _is_oauth:
        _enc_db, _ts_db = read_token_cache(cfg.source_name, log)
        if _enc_db:
            try:
                _raw_token   = decrypt_token(_enc_db, log)
                auth_headers = build_auth_headers_from_token(cfg.auth_type, _raw_token)
                _my_token_ts = _ts_db or run_start
                log.info(f"Auth: L2 token loaded from DB  auth_type={cfg.auth_type}")
            except Exception as _dec_exc:
                log.warning(f"L2 token decrypt failed — falling back to L3  error={_dec_exc}")
                auth_headers = get_auth_headers(cfg.auth_type, cfg.source_name, log, cfg.base_url)
                _my_token_ts = datetime.now(timezone.utc)
                log.info(f"Auth: L3 live fetch (L2 decrypt failed)  auth_type={cfg.auth_type}")
        else:
            auth_headers = get_auth_headers(cfg.auth_type, cfg.source_name, log, cfg.base_url)
            _my_token_ts = datetime.now(timezone.utc)
            log.info(f"Auth: L3 live fetch (no L2 cache)  auth_type={cfg.auth_type}")
    else:
        auth_headers = get_auth_headers(cfg.auth_type, cfg.source_name, log, cfg.base_url)
        log.info(f"Auth: L3 live fetch  auth_type={cfg.auth_type}")

    def token_refresher() -> dict:
        """
        On 401: jitter → re-read DB → use DB token if newer, else L3 + write DB.
        For non-OAuth types: straight L3 (credentials never expire mid-run).

        Jitter (1–3 s random sleep) staggers simultaneous 401 handlers so the
        first entity to complete the sleep finds the DB already updated by whoever
        won the race — most entities skip the L3 call entirely.
        """
        import random as _random
        import time   as _rt
        global _my_token_ts

        if not _is_oauth:
            return get_auth_headers(cfg.auth_type, cfg.source_name, log, cfg.base_url)

        _rt.sleep(_random.uniform(1.0, 3.0))

        _enc_db, _ts_db = read_token_cache(cfg.source_name, log)
        if _enc_db and _ts_db is not None and _ts_db > _my_token_ts:
            try:
                _raw = decrypt_token(_enc_db, log)
                _h   = build_auth_headers_from_token(cfg.auth_type, _raw)
                _my_token_ts = _ts_db
                log.info(f"Token refreshed from DB (peer refreshed first)  source={cfg.source_name}")
                return _h
            except Exception as _e:
                log.warning(f"DB token decrypt failed in refresher — falling back to L3  error={_e}")

        # First to refresh (or DB decrypt failed) — go to L3
        _new_h   = get_auth_headers(cfg.auth_type, cfg.source_name, log, cfg.base_url)
        _raw_new = _new_h["Authorization"].split(" ", 1)[1]
        _enc_new = encrypt_token(_raw_new, log)
        if _enc_new:
            write_token_cache(cfg.source_name, _enc_new, log)
        _my_token_ts = datetime.now(timezone.utc)
        log.info(f"Token refreshed from L3 and cached  source={cfg.source_name}")
        return _new_h

    # setdefault: lets source-specific Accept headers (e.g. application/vnd.github+json
    # returned by GITHUB_PAT / GITHUB_APP) win; falls back to application/json for all
    # other sources that don't set it themselves.
    auth_headers.setdefault("Accept", "application/json")

    paginator    = PAGINATION_DISPATCH[cfg.pagination_type]
    ts_label     = run_start.strftime("%Y%m%dT%H%M%SZ")
    landing_path = f"{cfg.landing_zone_path.rstrip('/')}/extracted_at={ts_label}/{RUN_ID}/"

    log.info(
        f"Extraction starting"
        f"  url={cfg.base_url}{cfg.endpoint_url}"
        f"  {cfg.watermark_column_name}>={cfg.watermark_column_value}"
    )

    # ── PAGINATE & WRITE TO LANDING ───────────────────────────────────────────

    _extra: dict = {}
    if cfg.pagination_type in ("Descriptor", "WIQL"):
        _extra["pagination_config"] = cfg.pagination_config

    page_iter = paginator(
        http_session,
        cfg.base_url,
        cfg.endpoint_url,
        auth_headers,
        cfg.watermark_column_name,
        _fmt_wm(cfg.watermark_column_value),
        log,
        token_refresher=token_refresher,
        **_extra,
    )
    records_loaded = write_pages_to_json(page_iter, landing_path, log, spark=spark)
    records_read   = records_loaded

    # ── ADVANCE WATERMARK ─────────────────────────────────────────────────────
    # Watermark is advanced BEFORE the audit SUCCESS row is written so the
    # final audit remarks always reflect the true outcome of this step.
    #
    # On watermark failure we do NOT raise — the landing write is already
    # committed and raising here would trigger a Workflow retry that re-lands
    # the same records as duplicates downstream.  Instead we log CRITICAL and
    # embed the manual-fix SQL in the audit remarks so ops can query:
    #
    #   SELECT run_id, remarks FROM ingestion_audit_log
    #   WHERE  remarks LIKE '%CRITICAL: watermark%'
    audit_remarks = (
        f"batch_run_id={batch_run_id} | Loaded {records_loaded} records → {landing_path}"
        if batch_run_id else
        f"Loaded {records_loaded} records → {landing_path}"
    )

    # NOTE: all paginators use >= (ge), so the record at max_wm is re-fetched on the
    # next run and lands again. The landing zone is intentionally raw/idempotent.
    # Silver-layer jobs must deduplicate on the entity primary key before aggregating.
    if records_read > 0:
        wm_ts = None
        try:
            max_row = (
                spark.read.option("mergeSchema", "true").json(landing_path)
                    .selectExpr(
                        f"max(CAST(`{cfg.watermark_column_name}` AS TIMESTAMP)) AS max_wm"
                    )
                    .first()
            )
            max_wm = max_row["max_wm"]

            if max_wm is not None:
                wm_ts = max_wm.strftime("%Y-%m-%d %H:%M:%S")  # space format — universally valid in TIMESTAMP '' literals
                spark.sql(f"""
                    UPDATE ingestion_entity_config
                    SET    watermark_column_value = TIMESTAMP '{wm_ts}'
                    WHERE  source_id = {source_id}
                      AND  entity_id = '{entity_id}'
                """)
                log.info(
                    f"Watermark advanced"
                    f"  column={cfg.watermark_column_name}"
                    f"  prev={cfg.watermark_column_value}"   # cfg is a snapshot — intentionally the old value
                    f"  new={wm_ts}"
                )
            else:
                log.warning(
                    f"Watermark not advanced — "
                    f"max(`{cfg.watermark_column_name}`) is NULL in landed data. "
                    "Verify watermark_column_name in ingestion_entity_config."
                )
                audit_remarks += " | WARNING: watermark column returned NULL"

        except Exception as wm_exc:
            wm_err = str(wm_exc)[:500]
            manual_fix = (
                f"UPDATE ingestion_entity_config "
                f"SET watermark_column_value = TIMESTAMP '{wm_ts}' "
                f"WHERE source_id = {source_id} AND entity_id = '{entity_id}'"
            ) if wm_ts else "wm_ts not computed — check watermark_column_name in config"
            log.critical(
                f"WATERMARK UPDATE FAILED — landing data committed but watermark NOT advanced. "
                f"Next run will re-fetch already-landed records. "
                f"Manual fix: {manual_fix}. "
                f"error={wm_err}"
            )
            audit_remarks += (
                f" | CRITICAL: watermark update failed — manual fix required: "
                f"{manual_fix}. error={wm_err[:200]}"
            )
    else:
        log.info("Watermark unchanged — no records extracted this run")

    # ── CLOSE AUDIT: SUCCESS ──────────────────────────────────────────────────
    # Wrapped in its own try/except so a transient Spark failure writing the
    # audit row does NOT cascade into the outer except block and produce a
    # false FAILED status — the landing write and watermark advance already
    # succeeded at this point.
    run_end = datetime.now(timezone.utc)
    try:
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
                remarks        = audit_remarks,
            ),
            log,
        )
        log.info(
            f"Audit closed  status=SUCCESS"
            f"  records_read={records_read}"
            f"  records_loaded={records_loaded}"
            f"  duration={round((run_end - run_start).total_seconds(), 2)}s"
        )
    except Exception as _success_audit_exc:
        log.warning(
            f"SUCCESS audit write failed — data landed and watermark advanced correctly. "
            f"RUNNING row remains in audit table.  error={_success_audit_exc}"
        )

except Exception as exc:
    run_end = datetime.now(timezone.utc)
    err_msg = str(exc)[:1000]

    try:
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
                records_failed = 0,
                file_path      = landing_path,
                remarks        = err_msg,
            ),
            log,
        )
    except Exception as _failed_audit_exc:
        log.warning(f"FAILED audit write failed — RUNNING row remains in audit table.  error={_failed_audit_exc}")
    log.error(
        f"Extraction failed"
        f"  duration={round((run_end - run_start).total_seconds(), 2)}s"
        f"  error={err_msg}"
    )

    raise


# COMMAND ----------

# DBTITLE 1,design
<title>Ingestion Framework</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap">

<style>
/* ── Tokens ─────────────────────────────────────────────────────────────────── */
:root {
  --bg:          #F7F8FA;
  --surface:     #FFFFFF;
  --surface-2:   #EEF1F7;
  --border:      #E0E4ED;
  --text:        #1A1D23;
  --text-2:      #596475;
  --accent:      #1C6EF2;
  --accent-muted:#EEF3FD;
  --code-bg:     #F0F2F7;
  --ok:          #1A8754;
  --ok-bg:       #ECFAF3;
  --warn:        #B45309;
  --warn-bg:     #FFF7ED;
  --crit:        #C0392B;
  --crit-bg:     #FEF2F2;
  --nav-w:       260px;
  color-scheme: light;
}

@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg:          #141720;
    --surface:     #1E2230;
    --surface-2:   #262B3A;
    --border:      #2E3347;
    --text:        #E8EAF0;
    --text-2:      #8A93A8;
    --accent:      #4B8EF7;
    --accent-muted:#1A2540;
    --code-bg:     #1A1F30;
    --ok:          #34D07A;
    --ok-bg:       #0E2A1C;
    --warn:        #F59E0B;
    --warn-bg:     #271D08;
    --crit:        #F87171;
    --crit-bg:     #2B1010;
    color-scheme: dark;
  }
}

:root[data-theme="dark"] {
  --bg:          #141720;
  --surface:     #1E2230;
  --surface-2:   #262B3A;
  --border:      #2E3347;
  --text:        #E8EAF0;
  --text-2:      #8A93A8;
  --accent:      #4B8EF7;
  --accent-muted:#1A2540;
  --code-bg:     #1A1F30;
  --ok:          #34D07A;
  --ok-bg:       #0E2A1C;
  --warn:        #F59E0B;
  --warn-bg:     #271D08;
  --crit:        #F87171;
  --crit-bg:     #2B1010;
  color-scheme: dark;
}

/* ── Reset ──────────────────────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; }

body {
  margin: 0;
  font-family: 'IBM Plex Sans', system-ui, sans-serif;
  font-size: 15px;
  line-height: 1.65;
  color: var(--text);
  background: var(--bg);
}

/* ── Layout ─────────────────────────────────────────────────────────────────── */
.shell {
  display: flex;
  min-height: 100vh;
}

/* ── Left nav ───────────────────────────────────────────────────────────────── */
.nav {
  width: var(--nav-w);
  flex-shrink: 0;
  position: sticky;
  top: 0;
  height: 100vh;
  overflow-y: auto;
  border-right: 1px solid var(--border);
  background: var(--surface);
  padding: 28px 0 40px;
  display: flex;
  flex-direction: column;
}

.nav-brand {
  padding: 0 22px 24px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 16px;
}

.nav-brand .wordmark {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-2);
}

.nav-brand .title {
  font-size: 14px;
  font-weight: 600;
  color: var(--text);
  margin-top: 4px;
}

.nav-group { margin-bottom: 4px; }

.nav-group-label {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text-2);
  padding: 6px 22px 4px;
  display: block;
}

.nav a {
  display: block;
  padding: 6px 22px 6px 28px;
  font-size: 13.5px;
  color: var(--text-2);
  text-decoration: none;
  border-left: 2px solid transparent;
  transition: color 0.12s, border-color 0.12s;
}

.nav a:hover,
.nav a.active {
  color: var(--accent);
  border-left-color: var(--accent);
  background: var(--accent-muted);
}

/* ── Main content ───────────────────────────────────────────────────────────── */
.content {
  flex: 1;
  min-width: 0;
  padding: 48px clamp(20px, 5vw, 72px) 80px;
  max-width: 900px;
}

/* ── Sections ───────────────────────────────────────────────────────────────── */
section { margin-bottom: 72px; }

.eyebrow {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 10.5px;
  font-weight: 500;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--accent);
  margin-bottom: 8px;
}

h1 {
  font-size: 32px;
  font-weight: 600;
  line-height: 1.25;
  letter-spacing: -0.02em;
  text-wrap: balance;
  color: var(--text);
  margin: 0 0 16px;
}

h2 {
  font-size: 22px;
  font-weight: 600;
  letter-spacing: -0.015em;
  text-wrap: balance;
  color: var(--text);
  margin: 0 0 16px;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--border);
}

h3 {
  font-size: 15px;
  font-weight: 600;
  color: var(--text);
  margin: 28px 0 10px;
}

p { margin: 0 0 14px; color: var(--text); max-width: 68ch; }

/* ── Lead ───────────────────────────────────────────────────────────────────── */
.lead {
  font-size: 16px;
  color: var(--text-2);
  max-width: 62ch;
  margin-bottom: 28px;
  line-height: 1.7;
}

/* ── Code ───────────────────────────────────────────────────────────────────── */
code {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 12.5px;
  background: var(--code-bg);
  color: var(--text);
  padding: 1px 5px;
  border-radius: 3px;
}

pre {
  background: var(--code-bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 16px 20px;
  overflow-x: auto;
  margin: 14px 0 20px;
}

pre code {
  background: none;
  padding: 0;
  font-size: 12.5px;
  line-height: 1.7;
}

/* ── Tables ─────────────────────────────────────────────────────────────────── */
.table-wrap { overflow-x: auto; margin: 14px 0 20px; }

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13.5px;
  min-width: 480px;
}

th {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 10.5px;
  font-weight: 500;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--text-2);
  padding: 9px 14px;
  text-align: left;
  border-bottom: 2px solid var(--border);
  background: var(--surface-2);
}

td {
  padding: 9px 14px;
  border-bottom: 1px solid var(--border);
  vertical-align: top;
  color: var(--text);
}

tr:last-child td { border-bottom: none; }
tr:hover td { background: var(--surface-2); }

/* ── Callouts ───────────────────────────────────────────────────────────────── */
.callout {
  border-left: 3px solid;
  border-radius: 0 6px 6px 0;
  padding: 12px 16px;
  margin: 16px 0;
  font-size: 13.5px;
  max-width: 68ch;
}

.callout.info  { border-color: var(--accent); background: var(--accent-muted); }
.callout.ok    { border-color: var(--ok);     background: var(--ok-bg);   }
.callout.warn  { border-color: var(--warn);   background: var(--warn-bg); }
.callout.crit  { border-color: var(--crit);   background: var(--crit-bg); }

.callout-label {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  margin-bottom: 4px;
  opacity: 0.75;
}

/* ── Badges ─────────────────────────────────────────────────────────────────── */
.badge {
  display: inline-block;
  font-family: 'IBM Plex Mono', monospace;
  font-size: 11px;
  font-weight: 500;
  padding: 2px 7px;
  border-radius: 3px;
  white-space: nowrap;
}

.badge-blue  { background: var(--accent-muted); color: var(--accent); }
.badge-green { background: var(--ok-bg);        color: var(--ok); }
.badge-amber { background: var(--warn-bg);      color: var(--warn); }
.badge-red   { background: var(--crit-bg);      color: var(--crit); }

/* ── Definition lists ───────────────────────────────────────────────────────── */
.def-list { margin: 14px 0 20px; }

.def-row {
  display: grid;
  grid-template-columns: 200px 1fr;
  gap: 0 24px;
  padding: 9px 0;
  border-bottom: 1px solid var(--border);
  align-items: baseline;
  font-size: 13.5px;
}

.def-row:last-child { border-bottom: none; }

.def-key {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 12px;
  color: var(--text);
}

.def-val { color: var(--text-2); }

/* ── Architecture SVG ───────────────────────────────────────────────────────── */
.arch-wrap {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 32px;
  margin: 20px 0;
  overflow-x: auto;
}

.arch-wrap svg { display: block; }

/* ── Flow steps ─────────────────────────────────────────────────────────────── */
.flow {
  display: flex;
  flex-direction: column;
  gap: 0;
  margin: 14px 0 20px;
}

.flow-step {
  display: flex;
  gap: 16px;
  position: relative;
}

.flow-step::before {
  content: '';
  position: absolute;
  left: 18px;
  top: 36px;
  bottom: -1px;
  width: 1px;
  background: var(--border);
}

.flow-step:last-child::before { display: none; }

.flow-num {
  flex-shrink: 0;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  background: var(--accent-muted);
  color: var(--accent);
  font-family: 'IBM Plex Mono', monospace;
  font-size: 13px;
  font-weight: 500;
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  z-index: 1;
}

.flow-body { padding: 7px 0 24px; min-width: 0; }

.flow-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--text);
  margin-bottom: 4px;
}

.flow-desc { font-size: 13.5px; color: var(--text-2); max-width: 60ch; }

/* ── File tree ──────────────────────────────────────────────────────────────── */
.file-tree {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 12.5px;
  background: var(--code-bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 16px 20px;
  line-height: 1.9;
  margin: 14px 0 20px;
}

.file-tree .dir { color: var(--text-2); }
.file-tree .file { color: var(--text); }
.file-tree .note { color: var(--text-2); font-size: 11.5px; }

/* ── Nav hamburger (mobile) ─────────────────────────────────────────────────── */
.nav-toggle {
  display: none;
  position: fixed;
  top: 16px;
  left: 16px;
  z-index: 200;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 10px;
  cursor: pointer;
  font-size: 18px;
  line-height: 1;
  color: var(--text);
}

@media (max-width: 720px) {
  .nav-toggle { display: block; }

  .nav {
    position: fixed;
    top: 0;
    left: 0;
    z-index: 100;
    transform: translateX(-100%);
    transition: transform 0.22s ease;
    height: 100dvh;
  }

  .nav.open { transform: translateX(0); }

  .content {
    padding-top: 64px;
    padding-left: 20px;
    padding-right: 20px;
  }
}

/* ── Focus ──────────────────────────────────────────────────────────────────── */
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
</style>

<button class="nav-toggle" onclick="document.querySelector('.nav').classList.toggle('open')" aria-label="Toggle navigation">☰</button>

<div class="shell">

<!-- ── LEFT NAV ─────────────────────────────────────────────────────────────── -->
<nav class="nav" id="sidenav">
  <div class="nav-brand">
    <div class="wordmark">Enterprise Ingestion</div>
    <div class="title">Framework Design</div>
  </div>

  <div class="nav-group">
    <span class="nav-group-label">Overview</span>
    <a href="#overview">Introduction</a>
    <a href="#structure">Repository Layout</a>
    <a href="#architecture">Architecture</a>
  </div>

  <div class="nav-group">
    <span class="nav-group-label">Components</span>
    <a href="#auth">Auth &amp; Token Cache</a>
    <a href="#http">HTTP Client</a>
    <a href="#paginators">Paginators</a>
    <a href="#landing">Landing Writer</a>
    <a href="#audit">Audit Log</a>
  </div>

  <div class="nav-group">
    <span class="nav-group-label">Data Model</span>
    <a href="#config-tables">Config Tables</a>
    <a href="#watermark">Watermark</a>
  </div>

  <div class="nav-group">
    <span class="nav-group-label">Guides</span>
    <a href="#adding-source">New Source</a>
    <a href="#adding-entity">New Entity</a>
    <a href="#operations">Operations</a>
  </div>
</nav>

<!-- ── CONTENT ──────────────────────────────────────────────────────────────── -->
<main class="content">

<!-- OVERVIEW ─────────────────────────────────────────────────────────────────── -->
<section id="overview">
  <div class="eyebrow">Enterprise Ingestion Framework</div>
  <h1>Design Document</h1>
  <p class="lead">A Databricks-native batch ingestion pipeline for enterprise SaaS APIs. One token fetch per batch run, config-driven pagination, and a structured audit trail — no custom code per new entity.</p>

  <h3>Purpose</h3>
  <p>The framework pulls data from external APIs (Azure DevOps, ServiceNow, GitHub) into a Databricks landing zone (raw Parquet). Each run is watermark-filtered — only records modified since the previous run are fetched. All credential handling uses Databricks Secrets; tokens never appear in workflow parameters, DBFS, or Delta tables.</p>

  <h3>Key properties</h3>
  <div class="def-list">
    <div class="def-row"><span class="def-key">Token prefetch</span><span class="def-val">Orchestrator fetches OAuth token once; all extractors share it via Databricks Secrets — 1 POST per batch, not 1 per entity.</span></div>
    <div class="def-row"><span class="def-key">Config-driven</span><span class="def-val">New entity = one DB row. Seven pagination strategies cover standard REST patterns without new Python code.</span></div>
    <div class="def-row"><span class="def-key">Idempotent writes</span><span class="def-val">Audit rows use MERGE on <code>run_id</code>. Landing path includes <code>RUN_ID</code> sub-folder — retries append safely.</span></div>
    <div class="def-row"><span class="def-key">Repair-run safe</span><span class="def-val"><code>_PREFETCH_IN_PROGRESS</code> forces a fresh token fetch even when a prior run's token is still valid in Secrets.</span></div>
    <div class="def-row"><span class="def-key">Memory-bounded</span><span class="def-val">Landing writer buffers at most 50 000 records (~50 MB) before flushing to Parquet. Large historical loads do not OOM the driver.</span></div>
  </div>
</section>

<!-- STRUCTURE ────────────────────────────────────────────────────────────────── -->
<section id="structure">
  <h2>Repository Layout</h2>

  <div class="file-tree">
<span class="dir">Main Notebook design/</span>
├── <span class="file">00_Orchestrator_Ingestion.py</span>   <span class="note">Batch entry point — query entities, prefetch token, exit with JSON payload</span>
├── <span class="file">01_Entity_Extractor.py</span>         <span class="note">Runs once per entity — auth, paginate, land, audit, advance watermark</span>
├── <span class="dir">_lib/</span>
│   ├── <span class="file">auth.py</span>                    <span class="note">OAuth helpers (ADO, SNOW, GitHub App, PAT) + 2-tier token cache</span>
│   ├── <span class="file">http_client.py</span>             <span class="note">Session factory, safe_get / safe_post with 429 + 401 handling</span>
│   ├── <span class="file">paginators.py</span>              <span class="note">7 pagination strategies + Descriptor generic; PAGINATION_DISPATCH map</span>
│   ├── <span class="file">landing_writer.py</span>          <span class="note">Chunked Parquet append to landing zone</span>
│   ├── <span class="file">audit.py</span>                   <span class="note">MERGE-based upsert to ingestion_audit_log</span>
│   └── <span class="file">logging_utils.py</span>           <span class="note">_setup_logger + ContextLogger (structured key=value context)</span>
├── <span class="dir">sample_data/</span>
│   └── <span class="file">workflow_definition.json</span>   <span class="note">Batch workflow — prepare_batch → For Each extract_entities</span>
└── <span class="dir">workflows/</span>
    ├── <span class="file">ingestion_ADO_BOARDS_WORKITEMS.json</span>
    ├── <span class="file">ingestion_SNOW_INCIDENTS.json</span>
    └── <span class="file">ingestion_GITHUB_REPOSITORIES.json</span>   <span class="note">Per-entity standalone workflows (no prefetch)</span>
  </div>
</section>

<!-- ARCHITECTURE ─────────────────────────────────────────────────────────────── -->
<section id="architecture">
  <h2>Architecture</h2>

  <div class="arch-wrap">
    <svg viewBox="0 0 780 500" width="100%" style="max-width:780px;font-family:'IBM Plex Sans',sans-serif">
      <defs>
        <marker id="arr" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#596475"/>
        </marker>
        <marker id="arr-b" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#1C6EF2"/>
        </marker>
      </defs>

      <!-- Databricks Workflow outer box -->
      <rect x="10" y="10" width="760" height="480" rx="8"
            fill="none" stroke="#E0E4ED" stroke-width="1.5"/>
      <text x="28" y="32" font-size="10" fill="#596475" font-family="'IBM Plex Mono',monospace"
            letter-spacing="0.08em" text-transform="uppercase">DATABRICKS WORKFLOW</text>

      <!-- Config tables (left side) -->
      <rect x="28" y="50" width="155" height="74" rx="5"
            fill="#F0F2F7" stroke="#E0E4ED" stroke-width="1"/>
      <text x="105" y="76" font-size="12" fill="#1A1D23" text-anchor="middle" font-weight="500">ingestion_source</text>
      <text x="105" y="92" font-size="12" fill="#1A1D23" text-anchor="middle" font-weight="500">_config</text>
      <text x="105" y="112" font-size="10.5" fill="#596475" text-anchor="middle">source_name · auth_type · base_url</text>

      <rect x="28" y="140" width="155" height="74" rx="5"
            fill="#F0F2F7" stroke="#E0E4ED" stroke-width="1"/>
      <text x="105" y="166" font-size="12" fill="#1A1D23" text-anchor="middle" font-weight="500">ingestion_entity</text>
      <text x="105" y="182" font-size="12" fill="#1A1D23" text-anchor="middle" font-weight="500">_config</text>
      <text x="105" y="202" font-size="10.5" fill="#596475" text-anchor="middle">entity_id · pagination · watermark</text>

      <!-- Arrow: config → orchestrator -->
      <line x1="183" y1="120" x2="224" y2="120" stroke="#596475" stroke-width="1.2" marker-end="url(#arr)"/>

      <!-- ORCHESTRATOR box -->
      <rect x="225" y="50" width="210" height="175" rx="6"
            fill="#EEF3FD" stroke="#1C6EF2" stroke-width="1.5"/>
      <text x="330" y="76" font-size="11.5" fill="#1C6EF2" text-anchor="middle" font-weight="600">00_Orchestrator</text>
      <text x="330" y="93" font-size="11.5" fill="#1C6EF2" text-anchor="middle" font-weight="600">_Ingestion.py</text>

      <text x="240" y="114" font-size="11" fill="#1A1D23">① Query active entities (JOIN)</text>
      <text x="240" y="131" font-size="11" fill="#1A1D23">② Prefetch OAuth token → L3</text>
      <text x="240" y="148" font-size="11" fill="#1A1D23">③ Write token to Secrets (L2)</text>
      <text x="240" y="165" font-size="11" fill="#1A1D23">④ Exit → entities JSON array</text>
      <text x="240" y="204" font-size="10.5" fill="#596475" font-style="italic">tokens never in output values</text>

      <!-- Arrow: orchestrator → Databricks Secrets -->
      <line x1="435" y1="115" x2="500" y2="115" stroke="#1C6EF2" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-b)"/>

      <!-- Databricks Secrets -->
      <rect x="500" y="80" width="175" height="70" rx="5"
            fill="#EEF3FD" stroke="#1C6EF2" stroke-width="1.5"/>
      <text x="587" y="107" font-size="11.5" fill="#1C6EF2" text-anchor="middle" font-weight="600">Databricks Secrets</text>
      <text x="587" y="124" font-size="10.5" fill="#596475" text-anchor="middle">scope: ingestion-secrets</text>
      <text x="587" y="139" font-size="10.5" fill="#596475" text-anchor="middle">{source}-prefetched-token</text>

      <!-- Arrow: orchestrator down → For Each -->
      <line x1="330" y1="225" x2="330" y2="265" stroke="#596475" stroke-width="1.2" marker-end="url(#arr)"/>
      <text x="340" y="252" font-size="10" fill="#596475">entities JSON</text>

      <!-- FOR EACH box -->
      <rect x="28" y="265" width="560" height="200" rx="6"
            fill="none" stroke="#E0E4ED" stroke-width="1.5" stroke-dasharray="6 3"/>
      <text x="44" y="283" font-size="10" fill="#596475" font-family="'IBM Plex Mono',monospace"
            letter-spacing="0.08em">FOR EACH ENTITY (concurrency = 4)</text>

      <!-- EXTRACTOR inner box -->
      <rect x="44" y="292" width="360" height="158" rx="5"
            fill="#FFFFFF" stroke="#E0E4ED" stroke-width="1.5"/>
      <text x="224" y="313" font-size="11.5" fill="#1A1D23" text-anchor="middle" font-weight="600">01_Entity_Extractor.py</text>

      <text x="60" y="332" font-size="11" fill="#1A1D23">① Read token from Secrets (L2) or fresh fetch (L3)</text>
      <text x="60" y="349" font-size="11" fill="#1A1D23">② Build HTTP session (retry + 429 back-off)</text>
      <text x="60" y="366" font-size="11" fill="#1A1D23">③ Paginate API → write_pages_to_parquet()</text>
      <text x="60" y="383" font-size="11" fill="#1A1D23">④ Read max(watermark_col) from landed Parquet</text>
      <text x="60" y="400" font-size="11" fill="#1A1D23">⑤ UPDATE ingestion_entity_config watermark</text>
      <text x="60" y="419" font-size="11" fill="#1A1D23">⑥ MERGE FAILED / SUCCESS → ingestion_audit_log</text>
      <text x="60" y="437" font-size="11" fill="#596475" font-style="italic">RUNNING row opened before ①, closed at ⑥</text>

      <!-- Secrets → Extractor arrow -->
      <line x1="587" y1="150" x2="587" y2="278" stroke="#1C6EF2" stroke-width="1.2" stroke-dasharray="4 3"/>
      <line x1="404" y1="313" x2="587" y2="278" stroke="#1C6EF2" stroke-width="1.2" stroke-dasharray="4 3" marker-end="url(#arr-b)"/>

      <!-- Extractor → Landing zone -->
      <line x1="404" y1="366" x2="468" y2="366" stroke="#596475" stroke-width="1.2" marker-end="url(#arr)"/>

      <!-- Landing zone -->
      <rect x="468" y="310" width="155" height="70" rx="5"
            fill="#F0F2F7" stroke="#E0E4ED" stroke-width="1"/>
      <text x="545" y="334" font-size="11.5" fill="#1A1D23" text-anchor="middle" font-weight="500">Landing Zone</text>
      <text x="545" y="351" font-size="10.5" fill="#596475" text-anchor="middle">Parquet / Delta Lake</text>
      <text x="545" y="366" font-size="10.5" fill="#596475" text-anchor="middle">extracted_at=…/{RUN_ID}/</text>

      <!-- Extractor → Audit log -->
      <line x1="404" y1="419" x2="468" y2="419" stroke="#596475" stroke-width="1.2" marker-end="url(#arr)"/>

      <!-- Audit log -->
      <rect x="468" y="393" width="155" height="56" rx="5"
            fill="#F0F2F7" stroke="#E0E4ED" stroke-width="1"/>
      <text x="545" y="418" font-size="11.5" fill="#1A1D23" text-anchor="middle" font-weight="500">ingestion_audit</text>
      <text x="545" y="434" font-size="11.5" fill="#1A1D23" text-anchor="middle" font-weight="500">_log</text>
      <text x="545" y="451" font-size="10.5" fill="#596475" text-anchor="middle">RUNNING → SUCCESS / FAILED</text>

      <!-- External API -->
      <rect x="680" y="305" width="82" height="60" rx="5"
            fill="#F7F8FA" stroke="#E0E4ED" stroke-width="1"/>
      <text x="721" y="330" font-size="11" fill="#596475" text-anchor="middle">External</text>
      <text x="721" y="345" font-size="11" fill="#596475" text-anchor="middle">API</text>
      <text x="721" y="356" font-size="10" fill="#596475" text-anchor="middle">(ADO/SNOW/GH)</text>

      <!-- Extractor ↔ API -->
      <line x1="620" y1="340" x2="680" y2="335" stroke="#596475" stroke-width="1.2" stroke-dasharray="3 2" marker-end="url(#arr)"/>
    </svg>
  </div>

  <h3>Batch run sequence</h3>
  <div class="flow">
    <div class="flow-step">
      <div class="flow-num">1</div>
      <div class="flow-body">
        <div class="flow-title">prepare_batch — query config</div>
        <div class="flow-desc">Orchestrator JOINs <code>ingestion_entity_config</code> and <code>ingestion_source_config</code> for all active entities under <code>source_id</code>. Raises if none found.</div>
      </div>
    </div>
    <div class="flow-step">
      <div class="flow-num">2</div>
      <div class="flow-body">
        <div class="flow-title">prepare_batch — token prefetch</div>
        <div class="flow-desc">Calls <code>prefetch_token()</code>. Bypasses L2 via <code>_PREFETCH_IN_PROGRESS</code> to force a fresh L3 fetch. Writes token + <code>expires_at</code> to Databricks Secrets as JSON.</div>
      </div>
    </div>
    <div class="flow-step">
      <div class="flow-num">3</div>
      <div class="flow-body">
        <div class="flow-title">prepare_batch → exit</div>
        <div class="flow-desc">Exits with <code>{"entities": [...]}</code>. The workflow reads <code>tasks.prepare_batch.values.entities</code> and fans it out to the For Each task.</div>
      </div>
    </div>
    <div class="flow-step">
      <div class="flow-num">4</div>
      <div class="flow-body">
        <div class="flow-title">extract_single_entity — per entity (×N parallel)</div>
        <div class="flow-desc">Each extractor opens an audit row (RUNNING), reads the token from Secrets (L2, or falls back to L3 on miss/expiry), paginates the API, writes Parquet, advances the watermark, and closes the audit row (SUCCESS or FAILED).</div>
      </div>
    </div>
  </div>
</section>

<!-- AUTH ──────────────────────────────────────────────────────────────────────── -->
<section id="auth">
  <h2>Auth &amp; Token Cache</h2>

  <p>All authentication is in <code>_lib/auth.py</code>. The framework supports four auth types. Each extractor process resolves the current token through a 2-tier lookup.</p>

  <h3>Supported auth types</h3>
  <div class="table-wrap">
    <table>
      <thead>
        <tr><th>auth_type</th><th>Flow</th><th>Secret keys required</th></tr>
      </thead>
      <tbody>
        <tr>
          <td><code>ADO_OAUTH</code></td>
          <td>Client-credentials POST to <code>login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token</code></td>
          <td><code>{source}-client-id</code>, <code>{source}-client-secret</code>, <code>{source}-tenant-id</code></td>
        </tr>
        <tr>
          <td><code>SNOW_OAUTH</code></td>
          <td>Client-credentials POST to <code>{base_url}/oauth_token.do</code></td>
          <td><code>{source}-client-id</code>, <code>{source}-client-secret</code>, <code>{source}-username</code>, <code>{source}-password</code></td>
        </tr>
        <tr>
          <td><code>GITHUB_APP</code></td>
          <td>RS256 JWT → installation token POST to <code>api.github.com/app/installations/{id}/access_tokens</code></td>
          <td><code>{source}-app-id</code>, <code>{source}-installation-id</code>, <code>{source}-private-key-pem</code></td>
        </tr>
        <tr>
          <td><code>GITHUB_PAT</code></td>
          <td>Personal access token — no token endpoint, reads secret directly</td>
          <td><code>{source}-pat</code></td>
        </tr>
      </tbody>
    </table>
  </div>

  <h3>2-tier token cache</h3>
  <p>The L1 in-process cache was deliberately removed. Each For Each iteration is its own Databricks process, so in-process caching offers no reuse. <code>get_auth_headers()</code> now goes through exactly two tiers:</p>

  <div class="table-wrap">
    <table>
      <thead>
        <tr><th>Tier</th><th>Store</th><th>Hit condition</th><th>Who writes</th></tr>
      </thead>
      <tbody>
        <tr>
          <td><span class="badge badge-blue">L2</span></td>
          <td>Databricks Secrets (<code>ingestion-secrets</code> scope)</td>
          <td>Key <code>{source}-prefetched-token</code> exists, JSON parses cleanly, <code>expires_at &gt; now()</code></td>
          <td>Orchestrator via <code>prefetch_token()</code></td>
        </tr>
        <tr>
          <td><span class="badge badge-amber">L3</span></td>
          <td>Token endpoint (network POST)</td>
          <td>L2 miss, expired, or <code>_PREFETCH_IN_PROGRESS</code> set</td>
          <td>Each extractor; also the orchestrator during prefetch</td>
        </tr>
      </tbody>
    </table>
  </div>

  <h3>Repair-run safety</h3>
  <p>On a Databricks Workflow repair run, only failed tasks re-execute. The orchestrator re-runs <code>prepare_batch</code>, which calls <code>prefetch_token()</code>. Without the bypass mechanism, the prior run's token (still valid in L2) would be served, <code>_FRESH_EXPIRES_IN</code> would be empty, and a default <code>expires_at</code> might exceed the token's actual remaining lifetime — causing every extractor to silently use an expired token until a 401.</p>

  <p>The fix: <code>prefetch_token()</code> adds <code>key_prefix</code> to <code>_PREFETCH_IN_PROGRESS</code> before calling <code>get_auth_headers()</code>, and removes it in the <code>finally</code> block. <code>_read_prefetched_token</code> returns <code>None</code> when the key is in that set, forcing L3 unconditionally.</p>

  <pre><code>_PREFETCH_IN_PROGRESS.add(key_prefix)
try:
    headers = get_auth_headers(auth_type, source_name, log, base_url)   # L2 bypassed
finally:
    _PREFETCH_IN_PROGRESS.discard(key_prefix)

expires_in = _FRESH_EXPIRES_IN.pop(key_prefix, _default[auth_type_upper])
expires_at  = time.time() + expires_in - 300   # 5-minute safety margin
_write_prefetched_token(key_prefix, token, expires_at, log)</code></pre>

  <h3>Secret storage for the prefetched token</h3>
  <p>Databricks does not expose a <code>dbutils.secrets.put()</code> API. The write uses the Databricks Secrets REST API directly, authenticated with the cluster's own API token:</p>

  <pre><code>POST https://{host}/api/2.0/secrets/put
Authorization: Bearer {cluster_api_token}
{
  "scope":        "ingestion-secrets",
  "key":          "{source}-prefetched-token",
  "string_value": '{"token": "...", "expires_at": 1758000000.0}'
}</code></pre>

  <div class="callout crit">
    <div class="callout-label">Security constraint</div>
    Tokens must never be passed as workflow parameters (visible in Jobs run-history UI), written to DBFS (plaintext, any cluster user can read), or stored in Delta tables (plaintext). Databricks Secrets is the only accepted store: encrypted at rest, ACL-controlled, and masked in logs.
  </div>
</section>

<!-- HTTP CLIENT ──────────────────────────────────────────────────────────────── -->
<section id="http">
  <h2>HTTP Client</h2>

  <p><code>_lib/http_client.py</code> provides two functions, <code>safe_get</code> and <code>safe_post</code>, on top of a retry-configured <code>requests.Session</code>.</p>

  <h3>Automatic retries (urllib3)</h3>
  <p><code>build_http_session()</code> mounts a <code>Retry</code> adapter with <code>status_forcelist={500, 502, 503, 504}</code> and <code>backoff_factor=2.0</code> (sleeps: 2 s, 4 s, 8 s). GET requests are retried automatically; POST is handled manually in <code>safe_post</code>.</p>

  <h3>Rate-limit handling (429)</h3>
  <p>429 is intentionally excluded from urllib3's <code>status_forcelist</code> so the <code>Retry-After</code> header is honoured. The manual loop in <code>safe_get</code> / <code>safe_post</code> parses both seconds and HTTP-date formats:</p>

  <pre><code>for attempt in range(1, max_waits + 1):   # default max_waits = 3
    resp = session.get(url, headers=headers, params=params, timeout=TIMEOUT)
    if resp.status_code == 429:
        wait = _parse_retry_after(resp)    # int seconds | HTTP-date → int
        log.warning(f"Rate-limited  attempt={attempt}/{max_waits}  sleeping={wait}s")
        if attempt &lt; max_waits:
            time.sleep(wait)
        continue
    ...</code></pre>

  <h3>Token refresh on 401</h3>
  <p>Each paginator receives an optional <code>token_refresher</code> callable. A single 401 triggers one token refresh and one immediate retry. A second consecutive 401 raises <code>HTTPError</code> immediately — no infinite retry loop.</p>

  <div class="def-list">
    <div class="def-row"><span class="def-key">TIMEOUT</span><span class="def-val"><code>(5, 60)</code> — 5 s connect, 60 s read</span></div>
    <div class="def-row"><span class="def-key">MAX_RETRIES</span><span class="def-val"><code>3</code> — automatic 5xx retries (urllib3)</span></div>
    <div class="def-row"><span class="def-key">BACKOFF_FACTOR</span><span class="def-val"><code>2.0</code> — exponential: 2 s, 4 s, 8 s</span></div>
    <div class="def-row"><span class="def-key">MAX_429_WAITS</span><span class="def-val"><code>3</code> — max attempts before RuntimeError on rate limit</span></div>
  </div>
</section>

<!-- PAGINATORS ───────────────────────────────────────────────────────────────── -->
<section id="paginators">
  <h2>Paginators</h2>

  <p>All paginators in <code>_lib/paginators.py</code> are Python generators — they yield <code>list[dict]</code> one page at a time, keeping at most <code>_PAGE_SIZE = 100</code> records in memory per page. The caller (<code>write_pages_to_parquet</code>) buffers and flushes to Parquet independently.</p>

  <div class="table-wrap">
    <table>
      <thead>
        <tr><th>pagination_type</th><th>Mechanism</th><th>Typical sources</th></tr>
      </thead>
      <tbody>
        <tr>
          <td><code>PageNumber</code></td>
          <td>Increments <code>?page=</code> until empty response</td>
          <td>Generic REST APIs</td>
        </tr>
        <tr>
          <td><code>OffsetPagination</code></td>
          <td>Increments <code>sysparm_offset</code> by 100</td>
          <td>ServiceNow Table API</td>
        </tr>
        <tr>
          <td><code>ContinuationToken</code></td>
          <td>Reads <code>x-ms-continuationtoken</code> response header</td>
          <td>Azure DevOps REST API</td>
        </tr>
        <tr>
          <td><code>OData</code></td>
          <td>Follows <code>@odata.nextLink</code> in response body</td>
          <td>ADO Analytics, MS Graph, Dynamics</td>
        </tr>
        <tr>
          <td><code>WIQL</code></td>
          <td>Two-step: POST WIQL → batch GET by ID (max 200/request)</td>
          <td>Azure DevOps Work Items</td>
        </tr>
        <tr>
          <td><code>LinkHeader</code></td>
          <td>Follows <code>rel="next"</code> from <code>Link</code> response header</td>
          <td>GitHub REST API (recommended over PageNumber)</td>
        </tr>
        <tr>
          <td><code>Descriptor</code></td>
          <td>JSON config in DB row — handles all of the above without new Python code</td>
          <td>Any source fitting a standard pattern</td>
        </tr>
      </tbody>
    </table>
  </div>

  <h3>Descriptor paginator</h3>
  <p>The Descriptor type reads a JSON string from <code>ingestion_entity_config.pagination_config</code> and constructs all query parameters at runtime. It supports four <code>next_signal</code> options (<code>empty_batch</code>, <code>link_header</code>, <code>odata_next_link</code>, <code>continuation_header</code>), four <code>filter_mode</code> options, and an <code>extra_params</code> dict for arbitrary fixed query parameters.</p>

  <pre><code>-- ServiceNow with compound query + field selection
{"page_size_param":  "sysparm_limit",
 "offset_param":     "sysparm_offset",
 "offset_mode":      "offset",
 "records_path":     "result",
 "next_signal":      "empty_batch",
 "filter_mode":      "sysparm",
 "filter_template":  "active=true^{col}>={val}",
 "extra_params": {"sysparm_fields": "sys_id,number,state,sys_updated_on",
                  "sysparm_display_value": "false"}}

-- GitHub Issues (Link-header cursor pagination)
{"page_size_param": "per_page",
 "offset_mode":     "none",
 "records_path":    null,
 "next_signal":     "link_header",
 "filter_mode":     "querystring"}</code></pre>

  <h3>Watermark normalisation</h3>
  <p>PySpark TIMESTAMP columns arrive as Python <code>datetime</code> objects when collected; string-typed watermarks from the orchestrator arrive in space format (<code>2026-01-01 10:00:00</code>). The extractor normalises both to ISO-8601 via <code>_fmt_wm()</code> before passing to any paginator, so OData <code>$filter</code>, GitHub <code>since</code>, and WIQL <code>&gt;=</code> literals all receive the correct format.</p>

  <div class="callout warn">
    <div class="callout-label">Safety cap</div>
    All paginators enforce <code>_MAX_PAGES = 10 000</code>. If an API genuinely has more pages, increase this constant in <code>paginators.py</code> and update the timeout in the workflow JSON.
  </div>
</section>

<!-- LANDING & AUDIT ──────────────────────────────────────────────────────────── -->
<section id="landing">
  <h2>Landing Writer</h2>

  <p><code>write_pages_to_parquet()</code> in <code>_lib/landing_writer.py</code> consumes the paginator generator and flushes records to Parquet in fixed 50 000-record chunks.</p>

  <div class="def-list">
    <div class="def-row"><span class="def-key">Landing path</span><span class="def-val"><code>{landing_zone_path}/extracted_at={YYYYMMDDTHHMMSSZ}/{RUN_ID}/</code></span></div>
    <div class="def-row"><span class="def-key">Chunk size</span><span class="def-val">50 000 records per Parquet file (~50 MB at 1 KB/record)</span></div>
    <div class="def-row"><span class="def-key">Write mode</span><span class="def-val"><code>append</code> — safe for retries since RUN_ID sub-folder is unique per run</span></div>
    <div class="def-row"><span class="def-key">Schema drift</span><span class="def-val">Schema inferred per chunk independently. Readers must use <code>spark.read.option("mergeSchema","true")</code>.</span></div>
  </div>

  <div class="callout info">
    <div class="callout-label">Downstream dedup requirement</div>
    All paginators use <code>&gt;=</code> (ge) in the watermark filter, so the record at the exact max-watermark timestamp is re-fetched on the next run and lands a second time. Silver-layer jobs must deduplicate on the entity primary key before aggregating.
  </div>
</section>

<section id="audit">
  <h2>Audit Log</h2>

  <p>Every extractor run writes to <code>ingestion_audit_log</code> via <code>upsert_audit()</code> in <code>_lib/audit.py</code>. The write is a Delta MERGE keyed on <code>run_id</code>, making every close idempotent — re-running a cell after a transient failure does not create duplicate rows.</p>

  <h3>Run lifecycle</h3>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Status</th><th>Written when</th><th>Note</th></tr></thead>
      <tbody>
        <tr>
          <td><span class="badge badge-blue">RUNNING</span></td>
          <td>Before the first API call</td>
          <td>A RUNNING row older than ~2 h indicates a stuck or killed run (cluster eviction, OOM)</td>
        </tr>
        <tr>
          <td><span class="badge badge-green">SUCCESS</span></td>
          <td>After watermark advance</td>
          <td>Includes <code>records_read</code>, <code>records_loaded</code>, <code>execution_duration_sec</code>, <code>file_path</code></td>
        </tr>
        <tr>
          <td><span class="badge badge-red">FAILED</span></td>
          <td>In the outer <code>except</code> block</td>
          <td><code>remarks</code> holds the exception string (truncated to 1 000 chars)</td>
        </tr>
      </tbody>
    </table>
  </div>

  <p>The RUNNING open-row write and the final SUCCESS close are both wrapped in their own <code>try/except</code> — a transient audit table failure does not cancel a successful extraction.</p>
</section>

<!-- CONFIG TABLES ────────────────────────────────────────────────────────────── -->
<section id="config-tables">
  <h2>Config Tables</h2>

  <h3>ingestion_source_config</h3>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>
      <tbody>
        <tr><td><code>source_id</code></td><td>INT PK</td><td>Numeric identifier; used as the orchestrator widget value</td></tr>
        <tr><td><code>source_name</code></td><td>STRING</td><td>Lowercase key used in Databricks Secrets key prefixes (e.g. <code>ado-boards</code>)</td></tr>
        <tr><td><code>base_url</code></td><td>STRING</td><td>Protocol + host only — no trailing slash (e.g. <code>https://dev.azure.com/myorg/myproject</code>)</td></tr>
        <tr><td><code>auth_type</code></td><td>STRING</td><td>One of: <code>ADO_OAUTH</code> <code>SNOW_OAUTH</code> <code>GITHUB_APP</code> <code>GITHUB_PAT</code></td></tr>
        <tr><td><code>is_active</code></td><td>BOOLEAN</td><td>Rows with <code>FALSE</code> are excluded from batch queries</td></tr>
      </tbody>
    </table>
  </div>

  <h3>ingestion_entity_config</h3>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>
      <tbody>
        <tr><td><code>entity_id</code></td><td>STRING PK</td><td>Unique identifier; alphanumeric + underscores only</td></tr>
        <tr><td><code>source_id</code></td><td>INT FK</td><td>References <code>ingestion_source_config.source_id</code></td></tr>
        <tr><td><code>endpoint_url</code></td><td>STRING</td><td>Path appended to <code>base_url</code> (e.g. <code>/repos/octo/hello/issues</code>)</td></tr>
        <tr><td><code>pagination_type</code></td><td>STRING</td><td>One of the 7 registered types (see Paginators section)</td></tr>
        <tr><td><code>pagination_config</code></td><td>STRING</td><td>JSON descriptor — required for <code>Descriptor</code> and <code>WIQL</code> types, NULL for others</td></tr>
        <tr><td><code>landing_zone_path</code></td><td>STRING</td><td>ABFSS or DBFS path for the raw Parquet files (no trailing slash)</td></tr>
        <tr><td><code>watermark_column_name</code></td><td>STRING</td><td>API filter parameter name (e.g. <code>since</code>, <code>ChangedDate</code>, <code>sys_updated_on</code>)</td></tr>
        <tr><td><code>watermark_column_value</code></td><td>TIMESTAMP</td><td>Updated after each successful run; used as the <code>&gt;=</code> filter on the next run</td></tr>
        <tr><td><code>active_flag</code></td><td>BOOLEAN</td><td>Rows with <code>FALSE</code> are skipped by the orchestrator</td></tr>
      </tbody>
    </table>
  </div>
</section>

<!-- WATERMARK ────────────────────────────────────────────────────────────────── -->
<section id="watermark">
  <h2>Watermark Strategy</h2>

  <p>The watermark is the high-water timestamp stored in <code>ingestion_entity_config.watermark_column_value</code>. After a successful run, the extractor reads the <em>maximum value of <code>watermark_column_name</code></em> from the landed Parquet and writes it back to the config table.</p>

  <div class="callout warn">
    <div class="callout-label">≥ boundary overlap</div>
    The watermark filter uses <code>&gt;=</code> (ge), so the record at the exact max-watermark timestamp is always re-fetched on the next run. This is intentional — it avoids the risk of dropping records updated at the same millisecond as the watermark. The cost is one duplicate record per run at the boundary. Deduplicate on entity primary key downstream.
  </div>

  <h3>Manual watermark reset</h3>
  <pre><code>-- Move watermark back 7 days to re-ingest the last week
UPDATE ingestion_entity_config
SET    watermark_column_value = TIMESTAMP '2026-09-16 00:00:00'
WHERE  entity_id = 'SNOW_INCIDENTS'
  AND  source_id = 5;</code></pre>

  <h3>WIQL watermark format</h3>
  <p>WIQL datetime literals require <code>YYYY-MM-DD HH:MM:SS</code> format (space separator, no <code>T</code>, no <code>Z</code>). The <code>_to_snow_dt()</code> helper converts the ISO-8601 string produced by <code>_fmt_wm()</code> to this format before interpolation into the WIQL query.</p>
</section>

<!-- ADDING SOURCE ────────────────────────────────────────────────────────────── -->
<section id="adding-source">
  <h2>Adding a New Source</h2>

  <div class="flow">
    <div class="flow-step">
      <div class="flow-num">1</div>
      <div class="flow-body">
        <div class="flow-title">Create secrets in Databricks</div>
        <div class="flow-desc">All secrets go under scope <code>ingestion-secrets</code>. Key names follow the pattern <code>{source_name}-{credential}</code> where <code>source_name</code> matches the value you'll insert in step 2.</div>
      </div>
    </div>
    <div class="flow-step">
      <div class="flow-num">2</div>
      <div class="flow-body">
        <div class="flow-title">Insert a row into ingestion_source_config</div>
        <pre><code>INSERT INTO ingestion_source_config VALUES (
  6,                                   -- source_id (next available int)
  'jira-cloud',                        -- source_name (matches secret key prefix)
  'https://yourcompany.atlassian.net', -- base_url (no trailing slash)
  'SNOW_OAUTH',                        -- auth_type (or whichever applies)
  TRUE
);</code></pre>
      </div>
    </div>
    <div class="flow-step">
      <div class="flow-num">3</div>
      <div class="flow-body">
        <div class="flow-title">Grant the cluster service principal WRITE on the secret scope</div>
        <div class="flow-desc">The orchestrator writes the prefetched token to Databricks Secrets using the cluster's own API token. The principal must have at least <code>WRITE</code> permission on the <code>ingestion-secrets</code> scope.</div>
      </div>
    </div>
    <div class="flow-step">
      <div class="flow-num">4</div>
      <div class="flow-body">
        <div class="flow-title">Add entities for the source (see next section)</div>
        <div class="flow-desc">One or more rows in <code>ingestion_entity_config</code> referencing the new <code>source_id</code>.</div>
      </div>
    </div>
  </div>
</section>

<!-- ADDING ENTITY ────────────────────────────────────────────────────────────── -->
<section id="adding-entity">
  <h2>Adding a New Entity</h2>

  <p>A new entity requires only one INSERT. No Python code changes unless the API uses a pagination pattern not covered by the Descriptor type.</p>

  <pre><code>INSERT INTO ingestion_entity_config VALUES (
  'JIRA_ISSUES',                                     -- entity_id
  6,                                                  -- source_id
  '/rest/api/3/search',                               -- endpoint_url
  'Descriptor',                                       -- pagination_type
  '{
     "page_size_param":  "maxResults",
     "offset_param":     "startAt",
     "offset_mode":      "offset",
     "records_path":     "issues",
     "next_signal":      "empty_batch",
     "filter_mode":      "querystring",
     "extra_params": {"jql": "project = MYPROJ AND updated >= {col}"}
   }',                                               -- pagination_config
  'abfss://raw@adls.dfs.core.windows.net/jira/issues', -- landing_zone_path
  'updated',                                          -- watermark_column_name
  TIMESTAMP '2024-01-01 00:00:00',                   -- watermark_column_value
  TRUE                                               -- active_flag
);</code></pre>

  <div class="callout info">
    <div class="callout-label">New pagination type</div>
    If no existing type fits, add a generator function to <code>_lib/paginators.py</code> and register it in <code>PAGINATION_DISPATCH</code>. The generator must accept the standard signature: <code>(session, base_url, endpoint, headers, wm_col, wm_val, log, token_refresher=None)</code>.
  </div>
</section>

<!-- OPERATIONS ──────────────────────────────────────────────────────────────── -->
<section id="operations">
  <h2>Operations</h2>

  <h3>Monitoring stuck runs</h3>
  <p>A RUNNING row older than ~2 hours indicates a run that was killed mid-flight (OOM, cluster eviction, job timeout). The RUNNING row is never auto-closed in this case.</p>
  <pre><code>SELECT run_id, entity_id, source_id, run_start_time,
       DATEDIFF(MINUTE, run_start_time, current_timestamp()) AS stuck_minutes_ago
FROM   ingestion_audit_log
WHERE  status = 'RUNNING'
  AND  run_start_time &lt; current_timestamp() - INTERVAL 2 HOURS
ORDER  BY run_start_time;</code></pre>

  <h3>Recent failures</h3>
  <pre><code>SELECT entity_id, run_start_time, execution_duration_sec, remarks
FROM   ingestion_audit_log
WHERE  status = 'FAILED'
  AND  run_start_time &gt; current_timestamp() - INTERVAL 7 DAYS
ORDER  BY run_start_time DESC;</code></pre>

  <h3>Audit write failed — RUNNING row stuck</h3>
  <p>If <code>upsert_audit</code> fails during the FAILED close (Delta table locked, schema mismatch), the log emits: <code>FAILED audit write failed — RUNNING row remains in audit table.</code> The extraction result is correct; only the audit row is stale. Close it manually:</p>
  <pre><code>MERGE INTO ingestion_audit_log AS t
USING (SELECT 'abc-run-id' AS run_id, 'FAILED' AS status,
              'Manual close — audit write failed' AS remarks) AS s
  ON  t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET t.status = s.status, t.remarks = s.remarks;</code></pre>

  <h3>Force watermark reset after a failed run</h3>
  <pre><code>UPDATE ingestion_entity_config
SET    watermark_column_value = TIMESTAMP '2026-09-01 00:00:00'
WHERE  entity_id = 'ADO_BOARDS_WORKITEMS'
  AND  source_id = 1;</code></pre>

  <h3>Prefetch write failed — token not in Secrets</h3>
  <p>The orchestrator logs: <code>Token prefetch write failed — entity extractors will fetch their own tokens.</code> This is non-fatal. Each extractor falls through to L3 (fresh POST). Check that the cluster service principal has <code>WRITE</code> permission on the <code>ingestion-secrets</code> scope.</p>

  <h3>WIQL 20 000-record cap warning</h3>
  <p>If the WIQL paginator logs <code>WIQL result hit the $top=20000 cap</code>, work items beyond 20 000 are silently excluded from that run. Narrow the <code>wiql_where</code> config (by area path, work item type, or date range) or split the entity into multiple rows in <code>ingestion_entity_config</code>.</p>

  <h3>Log level</h3>
  <p>Set the <code>log_level</code> job parameter to <code>DEBUG</code> to see per-page log lines from every paginator. Revert to <code>INFO</code> in production to reduce Databricks driver log volume.</p>

  <h3>Workflow variants</h3>
  <div class="table-wrap">
    <table>
      <thead><tr><th>File</th><th>Use case</th><th>Token prefetch</th></tr></thead>
      <tbody>
        <tr>
          <td><code>sample_data/workflow_definition.json</code></td>
          <td>Batch — all active entities for one <code>source_id</code> in parallel</td>
          <td><span class="badge badge-green">Yes (orchestrator)</span></td>
        </tr>
        <tr>
          <td><code>workflows/ingestion_*.json</code></td>
          <td>Standalone — single entity, scheduled or on-demand</td>
          <td><span class="badge badge-amber">No (extractor fetches L3)</span></td>
        </tr>
      </tbody>
    </table>
  </div>
</section>

</main>
</div>

<script>
// ── Sticky nav highlight ──────────────────────────────────────────────────────
(function () {
  const links = document.querySelectorAll('.nav a');
  const targets = Array.from(links).map(a => {
    const id = a.getAttribute('href').slice(1);
    return document.getElementById(id);
  }).filter(Boolean);

  function onScroll() {
    let active = targets[0];
    for (const t of targets) {
      if (t.getBoundingClientRect().top <= 80) active = t;
    }
    links.forEach(l => {
      l.classList.toggle('active', l.getAttribute('href') === '#' + (active && active.id));
    });
  }

  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();

  // Close mobile nav on link click
  links.forEach(l => l.addEventListener('click', () => {
    document.querySelector('.nav').classList.remove('open');
  }));
})();
</script>
