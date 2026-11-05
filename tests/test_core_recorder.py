"""Recorder tests for connector 5.x; skipped under 4.x."""

import importlib.util
from pathlib import Path

import pytest

from snowflake_utils.testing.vcr import _core_recorder

pytestmark = pytest.mark.skipif(
    not _core_recorder.supported(), reason="needs snowflake-connector-python 5.x"
)

PASSWORD = "s3cret-pw-123"


def _client_api():
    return importlib.import_module(
        "snowflake.connector._internal.api_client.client_api"
    )


def _connect():
    import snowflake.connector

    return snowflake.connector.connect(
        account="acct", user="u", host="127.0.0.1", port=1, protocol="http"
    )


def _run_query(sql="select 1"):
    conn = _connect()
    try:
        return conn.cursor().execute(sql).fetchall()
    finally:
        conn.close()


class _Poison:
    """Fails on any call: proves replay never reaches the upstream core."""

    def __getattr__(self, name):
        raise AssertionError(f"replay called the upstream core: {name}")


def test_record_then_replay_without_any_upstream(tmp_path):
    from tests import core_fake

    path = tmp_path / "cassette.yaml"
    with core_fake.installed([[1]]):
        with _core_recorder.cassette(path, rewrite=True):
            assert _run_query() == [(1,)]

    _client_api().core_driver.client = _Poison()
    with _core_recorder.cassette(path, rewrite=False):
        assert _run_query() == [(1,)]


def test_cassette_is_readable_and_matches_on_sql(tmp_path):
    from tests import core_fake

    path = tmp_path / "cassette.yaml"
    with core_fake.installed([[1]]):
        with _core_recorder.cassette(path, rewrite=True):
            _run_query()

    text = path.read_text()
    assert "select 1" in text
    assert "core://statement_execute_query" in text
    assert "core://result_set_get_stream" in text
    assert "core://rows" in text
    assert "!!binary" not in text

    _client_api().core_driver.client = _Poison()
    with _core_recorder.cassette(path, rewrite=False):
        with pytest.raises(RuntimeError, match="no recorded statement_set_sql_query"):
            _run_query("select 100")


def test_recorded_secrets_are_scrubbed_and_scan_clean(tmp_path):
    from snowflake_utils.testing.vcr._scan import scan_cassette
    from tests import core_fake

    path = tmp_path / "cassette.yaml"
    with core_fake.installed([[1]]):
        with _core_recorder.cassette(path, rewrite=True):
            import snowflake.connector

            conn = snowflake.connector.connect(
                account="acct",
                user="u",
                password=PASSWORD,
                host="127.0.0.1",
                port=1,
                protocol="http",
            )
            conn.close()

    text = path.read_text()
    assert PASSWORD not in text
    assert "ver:3-hint" not in text
    assert scan_cassette(path) == []


def test_account_and_user_values_are_redacted_everywhere(tmp_path):
    import snowflake.connector

    from tests import core_fake

    path = tmp_path / "cassette.yaml"
    with core_fake.installed([["person-4242"]]):
        with _core_recorder.cassette(path, rewrite=True):
            conn = snowflake.connector.connect(
                account="acct-4242",
                user="person-4242",
                host="127.0.0.1",
                port=1,
                protocol="http",
            )
            cur = conn.cursor()
            cur.execute("select 'x'")
            cur.fetchall()
            conn.close()

    text = path.read_text()
    assert "acct-4242" not in text
    assert "person-4242" not in text


INNER_TEST = """
import snowflake.connector


def test_query(snowflake_vcr_cassette):
    conn = snowflake.connector.connect(
        account="acct", user="u", host="127.0.0.1", port=1, protocol="http"
    )
    try:
        assert conn.cursor().execute("select 1").fetchall() == [(1,)]
    finally:
        conn.close()
"""

INNER_CONFTEST = """
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

if os.environ.get("SNOWFAKE") == "1":
    import core_fake

    core_fake.install([[1]])
"""


def _run(pytester, *args, **env):
    with pytest.MonkeyPatch.context() as mp:
        mp.delenv("CI", raising=False)
        mp.delenv("SNOWFAKE", raising=False)
        for key, value in env.items():
            mp.setenv(key, value)
        return pytester.runpytest_subprocess(*args)


def test_plugin_records_then_replays_offline(pytester):
    from tests import core_fake

    pytester.makepyfile(test_inner=INNER_TEST)
    pytester.makeconftest(INNER_CONFTEST)
    (pytester.path / "core_fake.py").write_text(Path(core_fake.__file__).read_text())

    recorded = _run(pytester, "--snowflake-vcr-mode=rewrite", SNOWFAKE="1")
    recorded.assert_outcomes(passed=1)

    cassette = pytester.path / "cassettes" / "test_query.yaml"
    text = cassette.read_text()
    assert "select 1" in text
    assert "core://statement_execute_query" in text

    replayed = _run(pytester)
    replayed.assert_outcomes(passed=1)
