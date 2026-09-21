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
import json   as _json
import time   as _time
import requests as _requests
from cryptography.hazmat.primitives            import hashes         as _hashes
from cryptography.hazmat.primitives            import serialization  as _serialization
from cryptography.hazmat.primitives.asymmetric import padding        as _padding

SECRET_SCOPE = "ingestion-secrets"   # Databricks secret scope name

# GitHub API version header — pin this so behaviour doesn't silently change
_GITHUB_API_VERSION = "2022-11-28"

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
    Installation tokens expire after exactly 1 hour.  The entity extractor passes
        token_refresher = lambda: get_auth_headers(cfg.auth_type, cfg.source_name, log, cfg.base_url)
    to every safe_get / safe_post call.  On any 401 response, the HTTP helpers call
    token_refresher() which re-enters this function, generates a new JWT, exchanges
    it for a fresh installation token, and injects the new Authorization header —
    all transparently, mid-run, without any changes to the calling notebook.

    Private key rotation: store the new PEM in the Databricks secret.  The next
    token refresh (on the next 401, or the next run) picks it up automatically.
    """
    app_id          = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-app-id")
    installation_id = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-installation-id")
    private_key_pem = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-private-key")

    jwt_token = _make_github_jwt(app_id, private_key_pem)

    resp = _requests.post(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        headers={
            "Authorization":        f"Bearer {jwt_token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": _GITHUB_API_VERSION,
        },
        timeout=(5, 30),
    )
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

    log.debug(
        f"GitHub App token acquired"
        f"  app_id={app_id}"
        f"  installation_id={installation_id}"
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
    Tokens expire after ~30 minutes.  The entity extractor passes
        token_refresher = lambda: get_auth_headers(cfg.auth_type, cfg.source_name, log, cfg.base_url)
    to every safe_get call.  On any 401 the HTTP helper calls token_refresher(),
    which re-enters this function and acquires a fresh token — all transparently,
    mid-run, without restarting the notebook.

    Secret rotation: update the client_secret in Databricks Secrets.  The next
    token refresh picks it up automatically; no notebook changes required.
    """
    client_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-client-id")
    client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{key_prefix}-client-secret")

    token_url = f"{base_url.rstrip('/')}/oauth_token.do"

    resp = _requests.post(
        token_url,
        # ServiceNow requires application/x-www-form-urlencoded, not JSON
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        },
        timeout=(5, 30),
    )

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

    log.debug(
        f"ServiceNow OAuth2 token acquired"
        f"  instance={base_url}"
        f"  expires_in={body.get('expires_in', 'unknown')}s"
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

    resp = _requests.post(
        token_url,
        # Must be application/x-www-form-urlencoded (not JSON)
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "https://app.vssps.visualstudio.com/.default",
        },
        timeout=(5, 30),
    )

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

    log.debug(
        f"ADO OAuth2 token acquired"
        f"  tenant_id={tenant_id}"
        f"  expires_in={body.get('expires_in', 'unknown')}s"
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
