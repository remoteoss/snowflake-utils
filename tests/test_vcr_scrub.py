import gzip
import json

import pytest
from vcr.request import Request

from snowflake_utils.testing.vcr import _scrub

LOGIN_URI = "https://acme-prod.snowflakecomputing.com/session/v1/login-request?x=1"
QUERY_URI = "https://acme-prod.snowflakecomputing.com/queries/v1/query-request"


def _request(uri, body=None, headers=None):
    return Request("POST", uri, body, headers or {})


def _login_body():
    data = {
        "ACCOUNT_NAME": "acme-prod",
        "LOGIN_NAME": "JANE",
        "PASSWORD": "hunter2",
        "TOKEN": "eyJhbGciOi.eyJzdWIi.sig",
        "PROOF_KEY": "proof",
        "AUTHENTICATOR": "EXTERNALBROWSER",
        "CLIENT_ENVIRONMENT": {"OS": "darwin"},
        "CLIENT_APP_ID": "PythonConnector",
    }
    return gzip.compress(json.dumps({"data": data}).encode())


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        (
            "https://acme-prod.snowflakecomputing.com/session",
            f"https://{_scrub.VOID}.snowflakecomputing.com/session",
        ),
        (
            "https://acme.eu-west-1.snowflakecomputing.com:443/x?a=1",
            f"https://{_scrub.VOID}.snowflakecomputing.com:443/x?a=1",
        ),
        (
            "https://bucket.s3.amazonaws.com/p/results/abc/main/data_0",
            f"https://{_scrub.VOID}.s3.amazonaws.com/p/results/{_scrub.VOID}/main/data_0",
        ),
        ("http://127.0.0.1:8080/queries", "http://127.0.0.1:8080/queries"),
    ],
)
def test_canonical_uri(uri, expected):
    assert _scrub.canonical_uri(uri) == expected


def test_authenticator_request_is_dropped():
    uri = "https://acme.snowflakecomputing.com/session/authenticator-request"
    assert _scrub.before_record_request(_request(uri, b"{}")) is None


def test_login_request_is_gunzipped_and_scrubbed():
    request = _scrub.before_record_request(
        _request(
            LOGIN_URI,
            _login_body(),
            {"Content-Encoding": "gzip", "Authorization": "Snowflake Token=abc"},
        )
    )
    text = request.body.decode() if isinstance(request.body, bytes) else request.body
    for secret in ("hunter2", "eyJhbGciOi", "proof", "JANE", "acme-prod"):
        assert secret not in text
    data = json.loads(text)["data"]
    assert data["PASSWORD"] == data["AUTHENTICATOR"] == _scrub.SCRUBBED
    assert "CLIENT_ENVIRONMENT" not in data
    assert data["CLIENT_APP_ID"] == "PythonConnector"
    assert not [
        h for h in request.headers if h.lower() in {"authorization", "content-encoding"}
    ]
    assert "acme-prod" not in request.uri


def test_response_tokens_and_signed_urls_are_scrubbed():
    body = {
        "data": {
            "token": "ver:3-hint:1",
            "masterToken": "ver:3-hint:2",
            "chunks": [{"url": "https://s3/x?Signature=abc123&X-Amz-Date=2026&k=v"}],
            "chunkHeaders": {"x-amz-server-side-encryption-customer-key": "k"},
            "queryId": "keep-me",
        }
    }
    response = {
        "headers": {"x-amz-request-id": ["r"], "Date": ["now"]},
        "body": {"string": json.dumps(body).encode()},
    }
    scrubbed = _scrub.before_record_response(response)
    text = scrubbed["body"]["string"].decode()
    for secret in ("ver:3-hint", "abc123", "customer-key"):
        assert secret not in text
    assert json.loads(text)["data"]["queryId"] == "keep-me"
    assert list(scrubbed["headers"]) == ["Date"]


def test_non_json_response_is_left_alone():
    response = {"headers": {}, "body": {"string": b"not json"}}
    assert _scrub.before_record_response(response)["body"]["string"] == b"not json"


def _query(sql):
    return _request(QUERY_URI, json.dumps({"sqlText": sql}).encode())


def test_sql_matcher_ignores_whitespace():
    _scrub.sql_matcher(_query("select  1\n"), _query("select 1"))


def test_sql_matcher_rejects_different_sql():
    with pytest.raises(AssertionError, match="SQL differs"):
        _scrub.sql_matcher(_query("select 1"), _query("select 2"))


def test_sql_matcher_only_applies_to_query_requests():
    _scrub.sql_matcher(_request(LOGIN_URI, b"a"), _request(LOGIN_URI, b"b"))
