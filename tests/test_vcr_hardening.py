import gzip
import json
import os
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pytest
import yaml
from vcr.request import Request

from snowflake_utils.testing.vcr import _recorder, _scrub, plugin
from snowflake_utils.testing.vcr._scan import scan_dir
from tests.test_vcr_plugin import INNER_TEST, PASSWORD, FakeSnowflake, _run

SCRUBBED = _scrub.SCRUBBED
VOID = _scrub.VOID


def _request(uri="https://acme.snowflakecomputing.com/x", body=None, headers=None):
    return Request("POST", uri, body, headers or {})


def _response(body, headers=None):
    text = body if isinstance(body, bytes) else json.dumps(body).encode()
    return {"headers": headers or {}, "body": {"string": text}}


def _scrubbed_json(body):
    return json.loads(_scrub.before_record_response(_response(body))["body"]["string"])


def test_cookie_and_cloud_security_headers_are_dropped():
    request = _scrub.before_record_request(
        _request(
            headers={
                "Cookie": "a=b",
                "x-amz-security-token": "t",
                "Content-Type": "x",
            }
        )
    )
    assert list(request.headers) == ["Content-Type"]
    response = _scrub.before_record_response(
        _response(b"", {"Set-Cookie": ["a=b"], "Date": ["d"]})
    )
    assert list(response["headers"]) == ["Date"]


def test_stage_credentials_are_scrubbed():
    body = {
        "data": {
            "stageInfo": {
                "creds": {
                    "AWS_KEY_ID": "AKIAABCDEFGHIJKLMNOP",
                    "AWS_SECRET_KEY": "s",
                    "AWS_TOKEN": "t",
                    "AZURE_SAS_TOKEN": "sas",
                    "GCS_ACCESS_TOKEN": "g",
                },
                "location": "bucket/prefix",
            },
            "encryptionMaterial": {"queryStageMasterKey": "k", "queryId": "q"},
        }
    }
    data = _scrubbed_json(body)["data"]
    assert set(data["stageInfo"]["creds"].values()) == {SCRUBBED}
    assert data["stageInfo"]["location"] == "bucket/prefix"
    assert data["encryptionMaterial"] == {
        "queryStageMasterKey": SCRUBBED,
        "queryId": "q",
    }


@pytest.mark.parametrize(
    "uri",
    [
        "https://x.blob.core.windows.net/c/f?sv=2020&sig=abcdef123456&se=1",
        "https://storage.googleapis.com/b/o?X-Goog-Signature=abcdef123456&X-Goog-Credential=c",
        "https://x/y?token=abcdef123456&sessionToken=abc&onetimetoken=abc",
    ],
)
def test_cloud_signing_and_token_query_params_are_removed(uri):
    request = _scrub.before_record_request(_request(uri))
    keys = {key.lower() for key in parse_qs(urlsplit(request.uri).query)}
    assert keys.isdisjoint(
        {"sig", "x-goog-signature", "x-goog-credential", "token", "sessiontoken"}
        | {"onetimetoken"}
    )


def test_secret_keys_match_case_insensitively():
    body = {"data": {"password": "p", "Token": "t", "nested": {"MASTERTOKEN": "m"}}}
    request = _scrub.before_record_request(_request(body=json.dumps(body).encode()))
    assert json.loads(request.body) == {
        "data": {
            "password": SCRUBBED,
            "Token": SCRUBBED,
            "nested": {"MASTERTOKEN": SCRUBBED},
        }
    }


def test_json_escaped_inside_a_string_is_scrubbed():
    cell = json.dumps({"token": "abc", "keep": 1})
    scrubbed = _scrubbed_json({"data": {"rowset": [[cell]]}})
    assert json.loads(scrubbed["data"]["rowset"][0][0]) == {
        "token": SCRUBBED,
        "keep": 1,
    }


def test_form_encoded_secrets_are_scrubbed_and_binary_bodies_untouched():
    request = _scrub.before_record_request(
        _request(body=b"grant=x&token=abc123&client_secret=zzz&keep=1")
    )
    assert request.body == b"grant=x&token=SCRUBBED&client_secret=SCRUBBED&keep=1"
    binary = bytes(range(128, 256))
    assert _scrub.before_record_response(_response(binary))["body"]["string"] == binary


@pytest.mark.parametrize(
    "host", ["ACME-PROD.SNOWFLAKECOMPUTING.COM", "acme.snowflakecomputing.cn"]
)
def test_uppercase_and_cn_hosts_are_canonicalized(host):
    scrubbed = _scrubbed_json({"data": {"rowset": [[f"https://{host}/x"]]}})
    text = json.dumps(scrubbed)
    assert "acme" not in text.lower()
    assert VOID in text


def test_app_urls_are_canonicalized():
    scrubbed = _scrubbed_json(
        {"data": {"rowset": [["https://app.snowflake.com/myorg/myacct/#/x"]]}}
    )
    assert "myorg" not in json.dumps(scrubbed)


def test_malformed_gzip_is_returned_raw():
    good = gzip.compress(b"{}")
    assert _scrub.plain_body(good) == b"{}"
    assert _scrub.plain_body(good + b"junk") == good + b"junk"
    assert _scrub.plain_body(b"\x1f\x8bnot gzip") == b"\x1f\x8bnot gzip"


def _write_cassette(tmp_path, request=None, response=None):
    interaction = {
        "request": {
            "method": "POST",
            "uri": "https://127.0.0.1/x",
            "headers": {},
            "body": "",
            **(request or {}),
        },
        "response": {
            "status": {"code": 200, "message": "OK"},
            "headers": {},
            "body": {"string": ""},
            **(response or {}),
        },
    }
    path = tmp_path / "test_case.yaml"
    path.write_text(yaml.safe_dump({"version": 1, "interactions": [interaction]}))
    return path


@pytest.mark.parametrize(
    ("request_part", "response_part", "kind"),
    [
        ({"uri": "https://x.okta.com/api/v1/authn"}, None, "unsupported-auth-flow"),
        (
            {"uri": "https://127.0.0.1/oauth/token-request"},
            None,
            "unsupported-auth-flow",
        ),
        (
            {"headers": {"x-amz-security-token": ["t"]}},
            None,
            "secret-header:x-amz-security-token",
        ),
        (None, {"headers": {"Set-Cookie": ["a=b"]}}, "secret-header:set-cookie"),
        (None, {"body": {"string": "AKIAABCDEFGHIJKLMNOP"}}, "aws-access-key-id"),
        (
            {"uri": "https://x.blob.core.windows.net/f?sig=abcdef123456"},
            None,
            "signed-url-param",
        ),
        (
            {"body": json.dumps({"MASTERTOKEN": "abcdef"})},
            None,
            "secret-field:mastertoken",
        ),
        (
            None,
            {"body": {"string": json.dumps({"d": json.dumps({"token": "abcdef"})})}},
            "secret-field:token",
        ),
        ({"body": "grant=x&token=abc123"}, None, "secret-param:token"),
    ],
)
def test_scanner_flags_leak_classes(tmp_path, request_part, response_part, kind):
    _write_cassette(tmp_path, request_part, response_part)
    assert kind in {finding.kind for finding in scan_dir(tmp_path)}


def test_scanner_ignores_yaml_that_is_not_a_cassette(tmp_path):
    (tmp_path / "config.yaml").write_text("settings: {a: 1}")
    assert scan_dir(tmp_path) == []


def test_legacy_plugin_is_reported_instead_of_crashing_pytest(monkeypatch):
    monkeypatch.setattr(plugin, "_legacy_plugin_installed", lambda: True)
    parser = mock.Mock()
    plugin.pytest_addoption(parser)
    parser.getgroup.assert_not_called()
    with pytest.raises(pytest.UsageError, match="snowflake-vcrpy"):
        plugin.pytest_configure(mock.Mock())


def test_https_stub_wraps_the_vendored_connections():
    import snowflake.connector.vendored.urllib3.connection as sf_conn

    _recorder.install_patches()
    stubs = _recorder.installed_stubs
    assert (
        stubs.VCRRequestsHTTPSConnection._baseclass is sf_conn.VerifiedHTTPSConnection
    )
    assert stubs.VCRRequestsHTTPConnection._baseclass is sf_conn.HTTPConnection


def test_overrides_disable_token_cache_and_platform_detection_then_restore():
    from snowflake.connector import connection, token_cache

    original_make = token_cache.TokenCache.make
    original_init = connection.SnowflakeConnection.__init__
    env = os.environ.get("SNOWFLAKE_DISABLE_PLATFORM_DETECTION")
    with _recorder.connector_overrides(replay=False):
        cache = token_cache.TokenCache.make()
        assert type(cache).__name__.lstrip("_") == "NoopTokenCache"
        assert os.environ["SNOWFLAKE_DISABLE_PLATFORM_DETECTION"] == "true"
    assert token_cache.TokenCache.make == original_make
    assert os.environ.get("SNOWFLAKE_DISABLE_PLATFORM_DETECTION") == env
    with pytest.raises(RuntimeError), _recorder.connector_overrides(replay=True):
        raise RuntimeError
    assert connection.SnowflakeConnection.__init__ is original_init
    assert token_cache.TokenCache.make == original_make


def test_replay_swaps_auth_kwargs_for_the_stub():
    from snowflake.connector import connection

    seen = {}

    def fake_init(self, *args, **kwargs):
        seen.update(kwargs)

    with mock.patch.object(connection.SnowflakeConnection, "__init__", fake_init):
        with _recorder.connector_overrides(replay=True):
            connection.SnowflakeConnection(
                account="a",
                user="u",
                password="p",
                authenticator="x",
                private_key_file="f",
            )
    assert set(seen) == {"account", "user", "auth_class"}
    assert type(seen["auth_class"]).__name__ == "StubAuth"


def test_build_vcr_configuration():
    recorder = _recorder.build_vcr(cassette_dir="d", record_mode="none")
    assert "ocsp.snowflakecomputing.com" in recorder.ignore_hosts
    assert "X-Amz-Signature" in recorder.filter_query_parameters
    assert recorder.decode_compressed_response is True
    assert "sql" in recorder.match_on
    relaxed = _recorder.build_vcr(cassette_dir="d", record_mode="none", match_sql=False)
    assert "sql" not in relaxed.match_on


@pytest.fixture
def fake_snowflake():
    server = FakeSnowflake()
    yield server
    server.stop()


def _record(pytester, server, *args, **env):
    return _run(pytester, server.port, {"password": PASSWORD}, *args, **env)


def test_record_mode_overrides_are_refused(pytester, fake_snowflake):
    marked = INNER_TEST.replace(
        "@pytest.mark.snowflake_vcr\n",
        '@pytest.mark.snowflake_vcr(record_mode="once")\n',
    )
    pytester.makepyfile(test_inner=marked)
    result = _record(pytester, fake_snowflake, "--snowflake-vcr-mode=rewrite")
    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*record_mode='once' is not supported*"])
    assert not (pytester.path / "cassettes").exists()


@pytest.mark.parametrize("name", ["victim", "../victim"])
def test_cassette_paths_cannot_escape_the_cassette_dir(pytester, fake_snowflake, name):
    victim = pytester.path / "victim.yaml"
    victim.write_text("keep")
    path = str(victim) if name == "victim" else name
    marked = INNER_TEST.replace(
        "@pytest.mark.snowflake_vcr\n", f"@pytest.mark.snowflake_vcr(path={path!r})\n"
    )
    pytester.makepyfile(test_inner=marked)
    result = _record(pytester, fake_snowflake, "--snowflake-vcr-mode=rewrite")
    result.assert_outcomes(errors=1)
    assert victim.read_text() == "keep"


def test_rewrite_without_traffic_does_not_crash(pytester):
    pytester.makepyfile(
        test_inner="""
        import pytest


        @pytest.mark.snowflake_vcr
        def test_no_traffic():
            pass
        """
    )
    result = _run(pytester, 1, {}, "--snowflake-vcr-mode=rewrite")
    result.assert_outcomes(passed=1)


def test_mode_can_come_from_the_environment(pytester, fake_snowflake):
    pytester.makepyfile(test_inner=INNER_TEST)
    _record(pytester, fake_snowflake, SNOWFLAKE_VCR_MODE="rewrite").assert_outcomes(
        passed=1
    )
    assert (pytester.path / "cassettes" / "test_query.yaml").exists()
    bad = _record(pytester, fake_snowflake, SNOWFLAKE_VCR_MODE="bogus")
    assert bad.ret == pytest.ExitCode.USAGE_ERROR
    bad.stderr.fnmatch_lines(["*SNOWFLAKE_VCR_MODE must be one of*"])


def test_selection_all_records_unmarked_tests_and_disable_records_none(
    pytester, fake_snowflake
):
    unmarked = INNER_TEST.replace("@pytest.mark.snowflake_vcr\n", "")
    pytester.makepyfile(test_inner=unmarked)
    _record(
        pytester,
        fake_snowflake,
        "--snowflake-vcr-mode=rewrite",
        "--snowflake-record-tests-selection=all",
    ).assert_outcomes(passed=1)
    cassette = pytester.path / "cassettes" / "test_query.yaml"
    assert cassette.exists()
    cassette.unlink()
    pytester.makepyfile(test_inner=INNER_TEST)
    _record(
        pytester,
        fake_snowflake,
        "--snowflake-vcr-mode=rewrite",
        "--snowflake-record-tests-selection=disable",
    ).assert_outcomes(passed=1)
    assert not cassette.exists()


def test_fixture_aliases_and_vcr_instance_are_available(pytester):
    pytester.makepyfile(
        test_inner="""
        def test_fixtures(vcr_cassette, snowflake_vcr_cassette, snowflake_vcr):
            assert vcr_cassette is snowflake_vcr_cassette
            assert hasattr(snowflake_vcr, "use_cassette")
        """
    )
    _run(pytester, 1, {}, "--snowflake-vcr-mode=rewrite").assert_outcomes(passed=1)
