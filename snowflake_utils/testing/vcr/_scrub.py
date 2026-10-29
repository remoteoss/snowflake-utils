import gzip
import json
import re
import zlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
    "X-Goog-Signature",
    "X-Goog-Credential",
    "sig",
    "token",
    "sessionToken",
    "onetimetoken",
    "access_token",
]
IGNORED_HOSTS = ["ocsp.snowflakecomputing.com"]

_FILTERED_PARAMS_LOWER = frozenset(param.lower() for param in FILTERED_QUERY_PARAMS)
_SECRET_HEADER_PREFIXES = (
    "authorization",
    "cookie",
    "set-cookie",
    "x-amz-security-token",
    "x-amz-server-side-encryption-customer",
    "x-amz-id-2",
    "x-amz-request-id",
    "x-amz-version-id",
    "x-goog-",
    "x-ms-encryption-key",
)
SECRET_KEYS = frozenset(
    {
        "token",
        "mastertoken",
        "idtoken",
        "mfatoken",
        "remmetoken",
        "qrmk",
        "sessiontoken",
        "oldsessiontoken",
        "accesstoken",
        "refreshtoken",
        "clientsecret",
        "password",
        "passcode",
        "proofkey",
        "loginname",
        "accountname",
        "awskeyid",
        "awssecretkey",
        "awstoken",
        "azuresastoken",
        "gcsaccesstoken",
        "querystagemasterkey",
        "masterkey",
        "privatekey",
        "privatekeypassword",
    }
)
_DROPPED_KEYS = frozenset({"chunkheaders"})
_SIGNED_URL_PARAM = re.compile(
    r"(Signature|AWSAccessKeyId|Expires|X-Amz-[A-Za-z-]+|X-Goog-[A-Za-z-]+"
    r"|(?<=[?&])(?:sig|token|sessionToken|onetimetoken|access_token))"
    r"=[^&\"\\\s]+"
)
_SECRET_PARAM_NAMES = (
    "password|passcode|token|sessiontoken|oldsessiontoken|access_token"
    "|refresh_token|client_secret|proof_key|onetimetoken"
)
_SECRET_FORM_FIELD = re.compile(rf"(?i)\b({_SECRET_PARAM_NAMES})=[^&\s\"]+")
_SECRET_JSON_FIELD = re.compile(
    r'(?i)("(?:password|passcode|token|mastertoken|sessiontoken|qrmk|proof_key)"'
    r'\s*:\s*)"[^"]*"'
)
_ACCOUNT_HOST = re.compile(
    r"(?<![A-Za-z0-9.-])(?!ocsp\.)[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*"
    r"\.snowflakecomputing\.(com|cn)",
    re.IGNORECASE,
)
_ACCOUNT_URL = re.compile(r"(app\.snowflake\.com)/[^/\s\"]+/[^/\s\"]+", re.IGNORECASE)
_AWS_SUFFIX = ".amazonaws.com"


def normalize_key(key) -> str:
    return re.sub(r"[_-]", "", str(key)).lower()


def is_secret_key(key) -> bool:
    return normalize_key(key) in SECRET_KEYS


def plain_body(body: bytes | str | None) -> bytes | None:
    if body is None:
        return None
    raw = body.encode() if isinstance(body, str) else body
    if raw[:2] != b"\x1f\x8b":
        return raw
    try:
        return gzip.decompress(raw)
    except (OSError, EOFError, zlib.error):
        return raw


def canonical_uri(uri: str) -> str:
    """Replace account-identifying labels with the all-zero UUID."""
    parts = urlsplit(uri)
    host = parts.hostname or ""
    path = parts.path
    account = re.fullmatch(r".+\.snowflakecomputing\.(com|cn)", host)
    if account and not host.startswith("ocsp."):
        host = f"{VOID}.snowflakecomputing.{account.group(1)}"
    elif host.endswith(_AWS_SUFFIX):
        host = VOID + "." + host.split(".", 1)[1]
        path = re.sub(r"(/results/)[^/]+", rf"\g<1>{VOID}", path)
    netloc = host if parts.port is None else f"{host}:{parts.port}"
    query = urlencode(
        [
            (key, value)
            for key, value in parse_qsl(parts.query, keep_blank_values=True)
            if key.lower() not in _FILTERED_PARAMS_LOWER
        ]
    )
    return urlunsplit(parts._replace(netloc=netloc, path=path, query=query))


def is_secret_header(name: str) -> bool:
    return name.lower().startswith(_SECRET_HEADER_PREFIXES)


def _scrub_string(text: str) -> str:
    stripped = text.lstrip()
    if stripped and stripped[0] in "{[":
        try:
            inner = json.loads(text)
        except ValueError:
            pass
        else:
            cleaned = _scrub_node(inner)
            if cleaned != inner:
                return json.dumps(cleaned)
    text = _SIGNED_URL_PARAM.sub(rf"\1={SCRUBBED}", text)
    text = _ACCOUNT_HOST.sub(
        lambda match: f"{VOID}.snowflakecomputing.{match.group(1).lower()}", text
    )
    return _ACCOUNT_URL.sub(rf"\1/{VOID}/{VOID}", text)


def _scrub_node(node):
    if isinstance(node, dict):
        return {
            key: SCRUBBED if is_secret_key(key) and value else _scrub_node(value)
            for key, value in node.items()
            if normalize_key(key) not in _DROPPED_KEYS
        }
    if isinstance(node, list):
        return [_scrub_node(item) for item in node]
    if isinstance(node, str):
        return _scrub_string(node)
    return node


def scrub_body(body: bytes, *, is_login: bool = False) -> bytes:
    """Scrub a JSON or text body. Binary bodies are returned untouched."""
    try:
        payload = json.loads(body)
    except ValueError:
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return body
        text = _SECRET_FORM_FIELD.sub(rf"\1={SCRUBBED}", text)
        text = _SECRET_JSON_FIELD.sub(rf'\1"{SCRUBBED}"', text)
        return _scrub_string(text).encode()
    payload = _scrub_node(payload)
    data = payload.get("data") if isinstance(payload, dict) else None
    if is_login and isinstance(data, dict):
        data["AUTHENTICATOR"] = SCRUBBED
        data.pop("CLIENT_ENVIRONMENT", None)
    return json.dumps(payload).encode()


def before_record_request(request):
    path = urlsplit(request.uri).path
    if path == AUTHENTICATOR_PATH:
        return None
    request.uri = canonical_uri(request.uri)
    body = plain_body(request.body)
    if body is not None:
        request.body = scrub_body(body, is_login=path == LOGIN_PATH)
        for name in [h for h in request.headers if h.lower() == "content-encoding"]:
            del request.headers[name]
    for name in [h for h in request.headers if is_secret_header(h)]:
        del request.headers[name]
    return request


def before_record_response(response):
    headers = {
        name: value
        for name, value in response["headers"].items()
        if not is_secret_header(name)
    }
    body = response["body"]["string"]
    if isinstance(body, bytes) and body[:2] == b"\x1f\x8b":
        decoded = plain_body(body)
        if decoded is not body:
            body = decoded
            headers = {
                name: value
                for name, value in headers.items()
                if name.lower() not in {"content-encoding", "content-length"}
            }
    response["headers"] = headers
    raw = body.encode() if isinstance(body, str) else body or b""
    response["body"]["string"] = scrub_body(raw)
    return response


def _sql_text(request) -> str | None:
    body = plain_body(request.body)
    if not body:
        return None
    try:
        sql = json.loads(body).get("sqlText")
    except (ValueError, AttributeError):
        return None
    return " ".join(sql.split()) if sql else None


def sql_matcher(r1, r2) -> None:
    if urlsplit(r1.uri).path != QUERY_PATH:
        return
    if _sql_text(r1) != _sql_text(r2):
        raise AssertionError(f"SQL differs: {_sql_text(r1)!r} != {_sql_text(r2)!r}")
