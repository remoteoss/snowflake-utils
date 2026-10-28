import gzip
import json
import re
from urllib.parse import urlsplit, urlunsplit

VOID = "00000000-0000-0000-0000-000000000000"
SCRUBBED = "SCRUBBED"

LOGIN_PATH = "/session/v1/login-request"
QUERY_PATH = "/queries/v1/query-request"
AUTHENTICATOR_PATH = "/session/authenticator-request"

FILTERED_QUERY_PARAMS = [
    "request_id",
    "requestId",
    "request_guid",
    "retryCount",
    "retryReason",
    "clientStartTime",
    "databaseName",
    "schemaName",
    "roleName",
    "warehouse",
    "Signature",
    "Expires",
    "AWSAccessKeyId",
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Security-Token",
    "X-Amz-Date",
]
IGNORED_HOSTS = ["ocsp.snowflakecomputing.com"]

_SECRET_HEADER_PREFIXES = (
    "authorization",
    "x-amz-server-side-encryption-customer",
    "x-amz-id-2",
    "x-amz-request-id",
    "x-amz-version-id",
)
_SECRET_LOGIN_FIELDS = ("PASSWORD", "TOKEN", "PROOF_KEY", "LOGIN_NAME", "ACCOUNT_NAME")
_SECRET_RESPONSE_FIELDS = frozenset(
    {"token", "masterToken", "idToken", "mfaToken", "remMeToken", "qrmk"}
)
_DROPPED_RESPONSE_FIELDS = frozenset({"chunkHeaders"})
_SIGNED_URL_PARAM = re.compile(
    r"(Signature|AWSAccessKeyId|Expires|X-Amz-[A-Za-z-]+)=[^&\"\\]+"
)
_SNOWFLAKE_SUFFIX = ".snowflakecomputing.com"
_AWS_SUFFIX = ".amazonaws.com"


def plain_body(body: bytes | str | None) -> bytes | None:
    if body is None:
        return None
    raw = body.encode() if isinstance(body, str) else body
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


def canonical_uri(uri: str) -> str:
    """Replace account-identifying labels with the all-zero UUID."""
    parts = urlsplit(uri)
    host = parts.hostname or ""
    path = parts.path
    if host.endswith(_SNOWFLAKE_SUFFIX) and not host.startswith("ocsp."):
        host = VOID + _SNOWFLAKE_SUFFIX
    elif host.endswith(_AWS_SUFFIX):
        host = VOID + "." + host.split(".", 1)[1]
        path = re.sub(r"(/results/)[^/]+", rf"\g<1>{VOID}", path)
    netloc = host if parts.port is None else f"{host}:{parts.port}"
    return urlunsplit(parts._replace(netloc=netloc, path=path))


def is_secret_header(name: str) -> bool:
    return name.lower().startswith(_SECRET_HEADER_PREFIXES)


def scrub_login_body(body: bytes) -> bytes:
    payload = json.loads(body)
    data = payload.get("data", {})
    for field in _SECRET_LOGIN_FIELDS:
        if field in data:
            data[field] = SCRUBBED
    data["AUTHENTICATOR"] = SCRUBBED
    data.pop("CLIENT_ENVIRONMENT", None)
    return json.dumps(payload).encode()


def _scrub_json(node):
    if isinstance(node, dict):
        return {
            key: SCRUBBED
            if key in _SECRET_RESPONSE_FIELDS and value
            else _scrub_json(value)
            for key, value in node.items()
            if key not in _DROPPED_RESPONSE_FIELDS
        }
    if isinstance(node, list):
        return [_scrub_json(item) for item in node]
    if isinstance(node, str):
        return _SIGNED_URL_PARAM.sub(rf"\1={SCRUBBED}", node)
    return node


def before_record_request(request):
    path = urlsplit(request.uri).path
    if path == AUTHENTICATOR_PATH:
        return None
    request.uri = canonical_uri(request.uri)
    body = plain_body(request.body)
    if body is not None:
        if path == LOGIN_PATH:
            body = scrub_login_body(body)
        request.body = body
        for name in [h for h in request.headers if h.lower() == "content-encoding"]:
            del request.headers[name]
    for name in [h for h in request.headers if is_secret_header(h)]:
        del request.headers[name]
    return request


def before_record_response(response):
    response["headers"] = {
        name: value
        for name, value in response["headers"].items()
        if not is_secret_header(name)
    }
    body = response["body"]["string"]
    try:
        text = body.decode() if isinstance(body, bytes) else body
        response["body"]["string"] = json.dumps(_scrub_json(json.loads(text))).encode()
    except ValueError:
        pass
    return response


def _sql_text(request) -> str | None:
    body = plain_body(request.body)
    if not body:
        return None
    sql = json.loads(body).get("sqlText")
    return " ".join(sql.split()) if sql else None


def sql_matcher(r1, r2) -> None:
    if urlsplit(r1.uri).path != QUERY_PATH:
        return
    if _sql_text(r1) != _sql_text(r2):
        raise AssertionError(f"SQL differs: {_sql_text(r1)!r} != {_sql_text(r2)!r}")
