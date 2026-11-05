"""Stand-in for the connector 5.x Rust core, for the recorder tests only.

It answers the protobuf calls the sync driver makes over one connect/execute/
fetch/close cycle and hands back a canned stream pointer.  Row values are
served by patching ``create_row_iterator`` because rows never cross the proto
seam (the core returns an Arrow stream pointer, not row bytes).
"""

import contextlib

from google.protobuf import message_factory
from snowflake.connector._internal.protobuf_gen import database_driver_v1_pb2 as pb

SERVICE = pb.DESCRIPTOR.services_by_name["DatabaseDriver"]
METHODS = {
    "".join("_" + ch.lower() if ch.isupper() else ch for ch in name).lstrip(
        "_"
    ): message_factory.GetMessageClass(desc.output_type)
    for name, desc in SERVICE.methods_by_name.items()
}
DB = pb.DatabaseHandle(id=1, magic=11)
CONN = pb.ConnectionHandle(id=2, magic=22)
STMT = pb.StatementHandle(id=3, magic=33)
RS = pb.ResultSetHandle(id=4, magic=44)


class FakeCore:
    def __init__(self, rows):
        self.rows = rows
        self.calls: list[str] = []

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        def call(request):
            return self.dispatch(name, request)

        return call

    def dispatch(self, name, request):
        self.calls.append(name)
        match name:
            case "database_new":
                return pb.DatabaseNewResponse(db_handle=DB)
            case "connection_new":
                return pb.ConnectionNewResponse(conn_handle=CONN)
            case "connection_get_info":
                return pb.ConnectionGetInfoResponse(
                    host="127.0.0.1",
                    port=1,
                    server_url="http://127.0.0.1:1",
                    session_token="ver:3-hint:session",
                    session_id=42,
                    account="acct",
                    user="u",
                    role="r",
                    database="d",
                    schema="s",
                    warehouse="wh",
                )
            case "connection_get_parameter":
                return pb.ConnectionGetParameterResponse(value="UTC")
            case "connection_heartbeat":
                return pb.ConnectionHeartbeatResponse(valid=True)
            case "statement_new":
                return pb.StatementNewResponse(stmt_handle=STMT)
            case "statement_execute_query":
                return pb.ExecuteQueryResponse(
                    single=pb.ResultSetResponse(
                        result_set_handle=RS,
                        result_descriptor=pb.ResultSetDescriptor(
                            query_id="qid-1",
                            row_count=len(self.rows),
                            query_result_format="json",
                            columns=[
                                pb.ColumnMetadata(
                                    name="1",
                                    type="fixed",
                                    precision=1,
                                    scale=0,
                                    nullable=False,
                                )
                            ],
                        ),
                    ),
                    request_id="req-1",
                )
            case "result_set_get_stream":
                return pb.ResultSetGetStreamResponse(
                    stream=pb.ArrowArrayStreamPtr(value=b"\x01" * 8)
                )
        return METHODS[name]()


class _Rows:
    def __init__(self, rows):
        self._rows = [tuple(row) for row in rows]

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    def fetch_many(self, size):
        return self._rows[:0]

    def fetch_all(self):
        return list(self._rows)


def install(rows):
    """Point the driver at the fake core and at *rows* for result iteration."""
    import snowflake.connector.cursor._base as base
    from snowflake.connector._internal.api_client.client_api import core_driver

    core_driver.client = FakeCore(rows)
    base.create_row_iterator = lambda stream_ptr, **kwargs: _Rows(rows)


@contextlib.contextmanager
def installed(rows):
    import snowflake.connector.cursor._base as base
    from snowflake.connector._internal.api_client.client_api import core_driver

    previous_core = core_driver.client
    previous_factory = base.create_row_iterator
    install(rows)
    try:
        yield
    finally:
        core_driver.client = previous_core
        base.create_row_iterator = previous_factory
