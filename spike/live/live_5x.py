import os

import pytest
import snowflake.connector


def _connect():
    return snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "replay-account"),
        user=os.environ.get("SNOWFLAKE_USER", "replay-user"),
        authenticator="externalbrowser",
        database="SANDBOX",
        authentication_timeout=300,
        login_timeout=330,
    )


@pytest.mark.snowflake_vcr
def test_select_literal():
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("select 1 as one, 'x' as s")
        assert cur.fetchall() == [(1, "x")]
    finally:
        conn.close()
