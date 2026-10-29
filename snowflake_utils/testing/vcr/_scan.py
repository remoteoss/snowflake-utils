import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from ._scrub import (
    SCRUBBED,
    VOID,
    is_secret_header,
    is_secret_key,
    normalize_key,
    plain_body,
)

_PATTERNS = {
    "jwt": re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}"),
    "session-token": re.compile(r"ver:\d+-hint:"),
    "private-key": re.compile(r"BEGIN [A-Z ]*PRIVATE KEY"),
    "aws-access-key-id": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "signed-url-param": re.compile(
        r"(?:Signature|AWSAccessKeyId|X-Amz-Signature|X-Amz-Credential"
        r"|X-Amz-Security-Token|X-Goog-Signature|X-Goog-Credential"
        r"|(?<=[?&])(?:sig|token|sessionToken|onetimetoken|access_token))"
        rf"=(?!{SCRUBBED})[^&\"\s]{{8,}}"
    ),
    "account-host": re.compile(
        rf"(?<![A-Za-z0-9.-])(?!ocsp\.)(?!{VOID}\.)[A-Za-z0-9-]+"
        r"(?:\.[A-Za-z0-9-]+)*\.snowflakecomputing\.(?:com|cn)",
        re.IGNORECASE,
    ),
    "account-url": re.compile(
        rf"app\.snowflake\.com/(?!{VOID}/)[^/\s\"]+/[^/\s\"]+", re.IGNORECASE
    ),
}
_SECRET_FORM_FIELD = re.compile(
    r"(?i)\b(password|passcode|token|sessiontoken|oldsessiontoken|access_token"
    rf"|refresh_token|client_secret|proof_key|onetimetoken)=(?!{SCRUBBED})[^&\s\"]+"
)


@dataclass(frozen=True)
class Finding:
    file: str
    interaction: int
    kind: str

    def __str__(self) -> str:
        return f"{self.file}: interaction {self.interaction}: {self.kind}"


def _text(body) -> str:
    raw = plain_body(body)
    return "" if raw is None else raw.decode("utf-8", errors="replace")


def _secret_field_kinds(node) -> set[str]:
    found = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if is_secret_key(key) and value not in (None, "", SCRUBBED):
                found.add(f"secret-field:{normalize_key(key)}")
            found |= _secret_field_kinds(value)
    elif isinstance(node, list):
        for item in node:
            found |= _secret_field_kinds(item)
    elif isinstance(node, str) and node.lstrip()[:1] in ("{", "["):
        try:
            found |= _secret_field_kinds(json.loads(node))
        except ValueError:
            pass
    return found


def _body_kinds(text: str) -> set[str]:
    try:
        return _secret_field_kinds(json.loads(text))
    except ValueError:
        return {
            f"secret-param:{match.group(1).lower()}"
            for match in _SECRET_FORM_FIELD.finditer(text)
        }


def _unsupported_flow(uri: str) -> bool:
    parts = urlsplit(uri)
    host = parts.hostname or ""
    return (
        host.endswith("okta.com")
        or parts.path.startswith("/oauth/")
        or parts.path.startswith("/api/v1/authn")
    )


def _interaction_kinds(interaction: dict) -> set[str]:
    request = interaction.get("request", {})
    response = interaction.get("response", {})
    uri = str(request.get("uri", ""))
    request_text = _text(request.get("body"))
    response_text = _text((response.get("body") or {}).get("string"))
    header_names = [*request.get("headers", {}), *response.get("headers", {})]
    header_values = [
        str(value)
        for source in (request.get("headers", {}), response.get("headers", {}))
        for value in source.values()
    ]
    haystacks = [uri, request_text, response_text, *header_values]
    kinds = {
        kind
        for kind, pattern in _PATTERNS.items()
        for text in haystacks
        if pattern.search(text)
    }
    kinds |= {
        f"secret-header:{name.lower()}"
        for name in header_names
        if is_secret_header(name)
    }
    kinds |= _body_kinds(request_text) | _body_kinds(response_text)
    if _unsupported_flow(uri):
        kinds.add("unsupported-auth-flow")
    return kinds


def scan_cassette(path: Path) -> list[Finding]:
    import yaml

    try:
        data = yaml.safe_load(path.read_text())
    except (yaml.YAMLError, UnicodeDecodeError):
        return [Finding(path.name, -1, "unparseable")]
    if not isinstance(data, dict) or "interactions" not in data:
        return []
    return [
        Finding(path.name, index, kind)
        for index, interaction in enumerate(data["interactions"] or [])
        for kind in sorted(_interaction_kinds(interaction))
    ]


def scan_dir(directory: Path) -> list[Finding]:
    return [
        finding
        for path in sorted(directory.rglob("*.yaml"))
        for finding in scan_cassette(path)
    ]
