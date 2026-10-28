import gzip
import json

import pytest
from vcr.request import Request
from vcr.serialize import serialize
from vcr.serializers import yamlserializer

from snowflake_utils.testing.vcr._cli import main
from snowflake_utils.testing.vcr._convert import convert_cassette
from snowflake_utils.testing.vcr._scan import scan_cassette, scan_dir

HOST = "https://acme-prod.snowflakecomputing.com"
PASSWORD = "hunter2-hunter2"


def _response(body: dict | str) -> dict:
    text = body if isinstance(body, str) else json.dumps(body)
    return {
        "status": {"code": 200, "message": "OK"},
        "headers": {"Date": ["now"], "x-amz-request-id": ["req-1"]},
        "body": {"string": text.encode()},
    }


def _legacy_cassette(path):
    login = {
        "data": {
            "ACCOUNT_NAME": "acme-prod",
            "LOGIN_NAME": "JANE",
            "PASSWORD": PASSWORD,
            "CLIENT_ENVIRONMENT": {"OS": "darwin"},
        }
    }
    interactions = [
        (
            Request(
                "POST",
                f"{HOST}/session/authenticator-request",
                gzip.compress(b'{"data": {"AUTHENTICATOR": "EXTERNALBROWSER"}}'),
                {"Content-Encoding": "gzip"},
            ),
            _response({"data": {"ssoUrl": "https://idp"}}),
        ),
        (
            Request(
                "POST",
                f"{HOST}/session/v1/login-request",
                gzip.compress(json.dumps(login).encode()),
                {"Content-Encoding": "gzip"},
            ),
            _response(
                {"data": {"token": "ver:3-hint:abc", "masterToken": "ver:3-hint:d"}}
            ),
        ),
        (
            Request(
                "POST",
                f"{HOST}/queries/v1/query-request",
                gzip.compress(b'{"sqlText": "select 1"}'),
                {"Content-Encoding": "gzip"},
            ),
            _response({"data": {"queryId": "q1"}}),
        ),
    ]
    path.write_text(
        serialize(
            {
                "requests": [request for request, _ in interactions],
                "responses": [response for _, response in interactions],
            },
            yamlserializer,
        )
    )


@pytest.fixture
def legacy_dir(tmp_path):
    _legacy_cassette(tmp_path / "test_legacy.yaml")
    return tmp_path


def test_scan_flags_unscrubbed_legacy_cassette(legacy_dir):
    kinds = {finding.kind for finding in scan_cassette(legacy_dir / "test_legacy.yaml")}
    assert {
        "login-field:PASSWORD",
        "login-field:LOGIN_NAME",
        "login-field:ACCOUNT_NAME",
        "response-field:token",
        "response-field:masterToken",
        "session-token",
        "account-host",
    } <= kinds


def test_findings_never_contain_secret_values(legacy_dir):
    report = "\n".join(str(finding) for finding in scan_dir(legacy_dir))
    for secret in (PASSWORD, "ver:3-hint:abc", "acme-prod", "JANE"):
        assert secret not in report


def test_convert_scrubs_and_drops_the_auth_phase(legacy_dir):
    path = legacy_dir / "test_legacy.yaml"
    convert_cassette(path)
    text = path.read_text()
    assert scan_cassette(path) == []
    assert PASSWORD not in text
    assert "authenticator-request" not in text
    assert "select 1" in text
    assert text.count("- request:") == 2


def test_convert_is_idempotent(legacy_dir):
    path = legacy_dir / "test_legacy.yaml"
    convert_cassette(path)
    once = path.read_text()
    convert_cassette(path)
    assert path.read_text() == once


def test_check_command_fails_on_secrets_and_passes_when_clean(legacy_dir, capsys):
    assert main(["check", str(legacy_dir)]) == 1
    assert "login-field:PASSWORD" in capsys.readouterr().out
    convert_cassette(legacy_dir / "test_legacy.yaml")
    assert main(["check", str(legacy_dir)]) == 0


def test_convert_command_converts_then_checks(legacy_dir, capsys):
    assert main(["convert", str(legacy_dir)]) == 0
    assert "converted 1 cassettes" in capsys.readouterr().out
    assert scan_dir(legacy_dir) == []


def test_unparseable_cassette_is_a_finding(tmp_path):
    (tmp_path / "broken.yaml").write_text("interactions: [unclosed")
    assert [f.kind for f in scan_dir(tmp_path)] == ["unparseable"]
