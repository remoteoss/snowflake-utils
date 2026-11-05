import os

import pytest
import snowflake.connector
from snowflake.connector.errors import ProgrammingError


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
def test_multi_chunk_rows():
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "select seq8() as n, 'row-' || seq8() as s "
            "from table(generator(rowcount => 300000))"
        )
        rows = cur.fetchall()
        assert len(rows) == 300000
        assert rows[0][1] == "row-0"
    finally:
        conn.close()


@pytest.mark.snowflake_vcr
def test_error_response_replays():
    conn = _connect()
    try:
        cur = conn.cursor()
        with pytest.raises(ProgrammingError):
            cur.execute("select * from sandbox.public.table_that_does_not_exist_1")
    finally:
        conn.close()


@pytest.mark.snowflake_vcr
def test_fetchmany_and_description():
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("select 1 as a, 'x' as b union all select 2, 'y'")
        assert [c.name for c in cur.description] == ["A", "B"]
        assert cur.fetchmany(1) == [(1, "x")]
        assert cur.fetchall() == [(2, "y")]
    finally:
        conn.close()
