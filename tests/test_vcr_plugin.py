import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from snowflake_utils.testing.vcr import _recorder

PASSWORD = "s3cret-pw-123"

INNER_TEST = """
import json
import os

import pytest
import snowflake.connector


@pytest.mark.snowflake_vcr
def test_query():
    conn = snowflake.connector.connect(
        host="127.0.0.1",
        port=int(os.environ["FAKE_PORT"]),
        protocol="http",
        account="acct",
        user="u",
        login_timeout=5,
        network_timeout=5,
        client_session_keep_alive=False,
        **json.loads(os.environ["AUTH_KWARGS"]),
    )
    assert conn.cursor().execute("select 1").fetchall() == [(1,)]
    conn.close()
"""

QUERY_RESPONSE = {
    "data": {
        "parameters": [],
        "rowtype": [
            {
                "name": "1",
                "database": "",
                "schema": "",
                "table": "",
                "nullable": False,
                "type": "fixed",
                "byteLength": None,
                "length": None,
                "scale": 0,
                "precision": 1,
                "collation": None,
            }
        ],
        "rowset": [["1"]],
        "total": 1,
        "returned": 1,
        "queryId": "qid-1",
        "queryResultFormat": "json",
    },
    "code": None,
    "message": None,
    "success": True,
}
LOGIN_RESPONSE = {
    "data": {
        "token": "ver:3-hint:session",
        "masterToken": "ver:3-hint:master",
        "sessionId": 42,
        "parameters": [],
        "sessionInfo": {},
    },
    "code": None,
    "message": None,
    "success": True,
}


class FakeSnowflake:
    def __init__(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                outer.paths.append(self.path.split("?")[0])
                path = self.path.split("?")[0]
                body = {
                    "/session/v1/login-request": LOGIN_RESPONSE,
                    "/queries/v1/query-request": QUERY_RESPONSE,
                }.get(path, {"data": None, "success": True})
                payload = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):
                pass

        self.paths: list[str] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def fake_snowflake():
    server = FakeSnowflake()
    yield server
    server.stop()


def _run(pytester, port, auth_kwargs, *args, **env):
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("FAKE_PORT", str(port))
        mp.setenv("AUTH_KWARGS", json.dumps(auth_kwargs))
        mp.setenv("NO_PROXY", "127.0.0.1")
        mp.delenv("CI", raising=False)
        for key, value in env.items():
            mp.setenv(key, value)
        return pytester.runpytest_subprocess(*args)


def test_record_then_replay_offline_with_a_different_auth(pytester, fake_snowflake):
    pytester.makepyfile(test_inner=INNER_TEST)

    recorded = _run(
        pytester,
        fake_snowflake.port,
        {"password": PASSWORD},
        "--snowflake-vcr-mode=rewrite",
    )
    recorded.assert_outcomes(passed=1)
    cassette = pytester.path / "cassettes" / "test_query.yaml"
    text = cassette.read_text()
    assert PASSWORD not in text
    assert "ver:3-hint" not in text
    assert "select 1" in text
    assert "/session/authenticator-request" not in text
    assert "/session/v1/login-request" in text

    fake_snowflake.stop()
    pem_path = pytester.path / "unused.p8"
    pem_path.write_text("not read on replay")
    replayed = _run(
        pytester,
        fake_snowflake.port,
        {"authenticator": "SNOWFLAKE_JWT", "private_key_file": str(pem_path)},
    )
    replayed.assert_outcomes(passed=1)


def test_replay_fails_when_sql_changes(pytester, fake_snowflake):
    pytester.makepyfile(test_inner=INNER_TEST)
    _run(
        pytester,
        fake_snowflake.port,
        {"password": PASSWORD},
        "--snowflake-vcr-mode=rewrite",
    ).assert_outcomes(passed=1)
    fake_snowflake.stop()
    pytester.makepyfile(test_inner=INNER_TEST.replace("select 1", "select 2"))
    replayed = _run(pytester, fake_snowflake.port, {"password": PASSWORD})
    replayed.assert_outcomes(failed=1)


def test_rewrite_replaces_instead_of_appending(pytester, fake_snowflake):
    pytester.makepyfile(test_inner=INNER_TEST)
    for _ in range(2):
        _run(
            pytester,
            fake_snowflake.port,
            {"password": PASSWORD},
            "--snowflake-vcr-mode=rewrite",
        ).assert_outcomes(passed=1)
    text = (pytester.path / "cassettes" / "test_query.yaml").read_text()
    assert text.count("/queries/v1/query-request") == 1


def test_rewrite_is_refused_in_ci(pytester):
    pytester.makepyfile(test_inner=INNER_TEST)
    result = _run(
        pytester,
        1,
        {},
        "--snowflake-vcr-mode=rewrite",
        CI="true",
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*Refusing to record*"])


def test_unsupported_connector_raises_clear_error(monkeypatch):
    monkeypatch.setattr(_recorder, "_installed", False)
    monkeypatch.setitem(
        sys.modules, "snowflake.connector.vendored.urllib3.connection", None
    )
    with pytest.raises(_recorder.UnsupportedConnectorError, match="4.x"):
        _recorder.install_patches()
