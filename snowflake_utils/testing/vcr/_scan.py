import json
import re
from dataclasses import dataclass
from pathlib import Path

from ._scrub import (
    LOGIN_PATH,
    SCRUBBED,
    SECRET_LOGIN_FIELDS,
    SECRET_RESPONSE_FIELDS,
    VOID,
    plain_body,
)

_PATTERNS = {
    "jwt": re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}"),
    "session-token": re.compile(r"ver:\d+-hint:"),
    "private-key": re.compile(r"BEGIN [A-Z ]*PRIVATE KEY"),
    "signed-url-param": re.compile(
        r"(?:Signature|AWSAccessKeyId|X-Amz-Signature|X-Amz-Credential"
        rf"|X-Amz-Security-Token)=(?!{SCRUBBED})[^&\"\s]{{8,}}"
    ),
    "account-host": re.compile(
        rf"(?<![A-Za-z0-9.-])(?!ocsp\.)(?!{VOID}\.)[A-Za-z0-9-]+"
        r"(?:\.[A-Za-z0-9-]+)*\.snowflakecomputing\.com"
    ),
}


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


def _unscrubbed_keys(node, keys) -> set[str]:
    found = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key in keys and value not in (None, "", SCRUBBED):
                found.add(key)
            found |= _unscrubbed_keys(value, keys)
    elif isinstance(node, list):
        for item in node:
            found |= _unscrubbed_keys(item, keys)
    return found


def _json_kinds(text: str, is_login_request: bool) -> set[str]:
    try:
        payload = json.loads(text)
    except ValueError:
        return set()
    kinds = {
        f"response-field:{key}"
        for key in _unscrubbed_keys(payload, SECRET_RESPONSE_FIELDS)
    }
    if is_login_request and isinstance(payload, dict):
        data = payload.get("data") or {}
        kinds |= {
            f"login-field:{key}" for key in _unscrubbed_keys(data, SECRET_LOGIN_FIELDS)
        }
    return kinds


def _interaction_kinds(interaction: dict) -> set[str]:
    request = interaction.get("request", {})
    response = interaction.get("response", {})
    headers = [
        str(value)
        for source in (request.get("headers", {}), response.get("headers", {}))
        for value in source.values()
    ]
    request_text = _text(request.get("body"))
    response_text = _text((response.get("body") or {}).get("string"))
    haystacks = [str(request.get("uri", "")), request_text, response_text, *headers]
    kinds = {
        kind
        for kind, pattern in _PATTERNS.items()
        for text in haystacks
        if pattern.search(text)
    }
    is_login = str(request.get("uri", "")).split("?")[0].endswith(LOGIN_PATH)
    kinds |= _json_kinds(request_text, is_login_request=is_login)
    kinds |= _json_kinds(response_text, is_login_request=False)
    return kinds


def scan_cassette(path: Path) -> list[Finding]:
    import yaml

    try:
        data = yaml.safe_load(path.read_text())
        interactions = data["interactions"]
    except (yaml.YAMLError, KeyError, TypeError):
        return [Finding(path.name, -1, "unparseable")]
    return [
        Finding(path.name, index, kind)
        for index, interaction in enumerate(interactions)
        for kind in sorted(_interaction_kinds(interaction))
    ]


def scan_dir(directory: Path) -> list[Finding]:
    return [
        finding
        for path in sorted(directory.rglob("*.yaml"))
        for finding in scan_cassette(path)
    ]
