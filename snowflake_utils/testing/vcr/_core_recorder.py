"""Record/replay for snowflake-connector-python 5.x.

5.x moved HTTP into a Rust core: HTTP payloads no longer exist in Python, so
the vendored-urllib3 patching in ``_recorder`` cannot apply.  What does cross
the boundary is the protobuf RPC surface of ``DatabaseDriverClient``, reached
through the settable ``core_driver.client``.  This module records that surface
plus the rows Python itself receives (the core hands Python an Arrow stream
*pointer*, never the row bytes, so rows are captured by teeing the row
iterator).
"""

import base64
import contextlib
import datetime
import decimal
import json
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from ._scrub import SCRUBBED, _scrub_string, is_secret_key, normalize_key

SCHEMA = "snowflake-utils/core-v1"
ROWS_URI = "core://rows"
_SQL_METHODS = (
    "statement_prepare",
    "statement_execute_query",
    "statement_execute_async",
)


def connector_version() -> str:
    try:
        return version("snowflake-connector-python")
    except PackageNotFoundError:
        return "not installed"


def _major() -> int | None:
    raw = connector_version().split(".")[0]
    return int(raw) if raw.isdigit() else None


def supported() -> bool:
    """True when the installed connector exposes the 5.x core client seam."""
    if (_major() or 0) < 5:
        return False
    try:
        from snowflake.connector._internal.api_client import client_api  # noqa: F401
        from snowflake.connector._internal.protobuf_gen import (  # noqa: F401
            database_driver_v1_pb2,
        )
    except ImportError:
        return False
    return True


def _methods() -> dict:
    from google.protobuf import message_factory
    from snowflake.connector._internal.protobuf_gen import database_driver_v1_pb2 as pb

    service = pb.DESCRIPTOR.services_by_name["DatabaseDriver"]
    methods = {}
    for name, desc in service.methods_by_name.items():
        snake = "".join("_" + ch.lower() if ch.isupper() else ch for ch in name).lstrip(
            "_"
        )
        methods[snake] = message_factory.GetMessageClass(desc.output_type)
    return methods


def _norm_sql(text: str | None) -> str:
    return " ".join((text or "").split())


def _handle(handle) -> str:
    return f"{getattr(handle, 'id', '')}:{getattr(handle, 'magic', '')}"


_IDENTITY_KEYS = frozenset({"account", "user", "accountname", "username"})
MIN_SUBSTRING_IDENTITY = 6


def _scrub_node(node):
    if isinstance(node, dict):
        cleaned = {}
        for key, value in node.items():
            if is_secret_key(key) or normalize_key(key) in _IDENTITY_KEYS:
                cleaned[key] = _scrub_secret(value)
            else:
                cleaned[key] = _scrub_node(value)
        return cleaned
    if isinstance(node, list):
        return [_scrub_node(item) for item in node]
    if isinstance(node, str):
        return _scrub_string(node)
    return node


def _scrub_secret(value):
    if isinstance(value, dict):
        return {key: _scrub_secret(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub_secret(item) for item in value]
    return SCRUBBED


def _encode(value):
    if isinstance(value, (bytes, bytearray)):
        return {"__bytes__": base64.b64encode(bytes(value)).decode()}
    if isinstance(value, decimal.Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, datetime.datetime):
        return {"__datetime__": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"__date__": value.isoformat()}
    if isinstance(value, datetime.time):
        return {"__time__": value.isoformat()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _encode(item) for key, item in value.items()}
    return value


def _decode(value):
    if isinstance(value, dict):
        if len(value) == 1:
            if "__bytes__" in value:
                return base64.b64decode(value["__bytes__"])
            if "__decimal__" in value:
                return decimal.Decimal(value["__decimal__"])
            if "__datetime__" in value:
                return datetime.datetime.fromisoformat(value["__datetime__"])
            if "__date__" in value:
                return datetime.date.fromisoformat(value["__date__"])
            if "__time__" in value:
                return datetime.time.fromisoformat(value["__time__"])
        return {key: _decode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode(item) for item in value]
    return value


class _Iterator:
    """Row iterator over recorded rows, matching ``ArrowRowIterator``."""

    def __init__(self, rows):
        self._rows = list(rows)
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._rows):
            raise StopIteration
        row = self._rows[self._index]
        self._index += 1
        return tuple(row)

    def fetch_many(self, size):
        rows = [tuple(row) for row in self._rows[self._index : self._index + size]]
        self._index += len(rows)
        return rows

    def fetch_all(self):
        rows = [tuple(row) for row in self._rows[self._index :]]
        self._index = len(self._rows)
        return rows


class _TeeIterator(_Iterator):
    """Pass-through row iterator that copies every row it yields."""

    def __init__(self, inner, sink):
        self._inner = inner
        self._sink = sink

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self._inner)
        self._sink.append(row)
        return row

    def fetch_many(self, size):
        rows = self._inner.fetch_many(size)
        self._sink.extend(rows)
        return rows

    def fetch_all(self):
        rows = self._inner.fetch_all()
        self._sink.extend(rows)
        return rows


class _Seam:
    """Duck-typed replacement for ``DatabaseDriverClient``."""

    def __init__(self):
        self.sql: dict[str, str] = {}

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        def call(request, *args, **kwargs):
            return self.dispatch(name, request)

        return call

    def key(self, name, request, match_sql):
        if name == "statement_set_sql_query":
            self.sql[_handle(request.stmt_handle)] = _norm_sql(request.query)
            return _norm_sql(request.query)
        if name in _SQL_METHODS and match_sql:
            return self.sql.get(_handle(request.stmt_handle), "")
        return ""


class _Recording(_Seam):
    def __init__(self, client, *, match_sql: bool):
        super().__init__()
        self._client = client
        self._match_sql = match_sql
        self.interactions: list[dict] = []
        self.rows: list[list] = []
        self._identities: set[str] = set()

    def _learn_identities(self, request) -> None:
        options = _message_dict(request).get("options", {})
        for key, option in options.items():
            value = option.get("string_value") if isinstance(option, dict) else None
            if normalize_key(key) in _IDENTITY_KEYS and value:
                self._identities.add(value)

    def _redact(self, node):
        if isinstance(node, dict):
            return {key: self._redact(value) for key, value in node.items()}
        if isinstance(node, list):
            return [self._redact(item) for item in node]
        if not isinstance(node, str):
            return node
        for value in sorted(self._identities, key=len, reverse=True):
            if node == value:
                return SCRUBBED
            if len(value) >= MIN_SUBSTRING_IDENTITY:
                node = node.replace(value, SCRUBBED)
        return node

    def new_row_batch(self):
        batch: list = []
        self.rows.append(batch)
        return batch

    def dispatch(self, name, request):
        key = self.key(name, request, self._match_sql)
        if name == "connection_set_options":
            self._learn_identities(request)
        response = getattr(self._client, name)(request)
        body = json.dumps(self._redact(_scrub_node(_message_dict(request))))
        reply = json.dumps(self._redact(_scrub_node(_message_dict(response))))
        self.interactions.append(
            {
                "request": {
                    "uri": f"core://{name}",
                    "method": "CORE",
                    "key": key,
                    "body": body,
                },
                "response": {"headers": {}, "body": {"string": reply}},
            }
        )
        return response

    def to_document(self) -> dict:
        interactions = list(self.interactions)
        if self.rows:
            interactions.append(
                {
                    "request": {
                        "uri": ROWS_URI,
                        "method": "CORE",
                        "body": "[]",
                    },
                    "response": {
                        "headers": {},
                        "body": {
                            "string": json.dumps(
                                self._redact(
                                    [
                                        [
                                            [_encode(value) for value in row]
                                            for row in batch
                                        ]
                                        for batch in self.rows
                                    ]
                                )
                            )
                        },
                    },
                }
            )
        return {
            "schema": SCHEMA,
            "connector": connector_version(),
            "interactions": interactions,
        }


class _Replay(_Seam):
    def __init__(self, store):
        super().__init__()
        self._store = store

    def dispatch(self, name, request):
        key = self.key(name, request, self._store.match_sql)
        reply = self._store.response(name, key, request)
        return _parse_message(name, reply)


def _message_dict(message) -> dict:
    from google.protobuf.json_format import MessageToDict

    return MessageToDict(message, preserving_proto_field_name=True)


def _parse_message(name, payload: dict):
    from google.protobuf.json_format import ParseDict

    message = _methods()[name]()
    ParseDict(payload, message)
    return message


class _Store:
    def __init__(self, document: dict, *, match_sql: bool):
        self.match_sql = match_sql
        self._requested: dict[tuple[str, str], list[dict]] = {}
        self.rows: list[list] = []
        self._row_index = 0
        for interaction in document.get("interactions") or []:
            uri = str(interaction.get("request", {}).get("uri", ""))
            reply = json.loads(interaction["response"]["body"]["string"] or "null")
            if uri == ROWS_URI:
                self.rows = reply or []
                continue
            name = uri.removeprefix("core://")
            key = str(interaction["request"].get("key", ""))
            self._requested.setdefault((name, key), []).append(reply)

    def response(self, name, key, request):
        entries = self._requested.get((name, key))
        if entries is None and key:
            entries = self._requested.get((name, ""))
        if not entries:
            available = sorted({method for method, _ in self._requested})
            raise RuntimeError(
                f"no recorded {name} interaction (key={key!r}); "
                f"recorded methods: {available}"
            )
        return entries.pop(0) if len(entries) > 1 else entries[0]

    def next_row_batch(self):
        if self._row_index >= len(self.rows):
            return []
        batch = self.rows[self._row_index]
        self._row_index += 1
        return batch


class _Cassette:
    def __init__(self, path: Path):
        self._path = path


@contextlib.contextmanager
def _row_source(factory):
    import snowflake.connector.cursor._base as base

    original = base.create_row_iterator
    base.create_row_iterator = factory(original)
    try:
        yield
    finally:
        base.create_row_iterator = original


@contextlib.contextmanager
def cassette(path: Path, *, rewrite: bool, match_sql: bool = True):
    """Record or replay a 5.x session, writing/reading *path*."""
    import yaml
    from snowflake.connector._internal.api_client.client_api import core_driver

    previous = core_driver.client
    if rewrite:
        recorder = _Recording(previous, match_sql=match_sql)
        core_driver.client = recorder
        try:
            with _row_source(lambda real: _tee_factory(real, recorder)):
                yield _Cassette(path)
        finally:
            core_driver.client = previous
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(recorder.to_document(), sort_keys=False))
        return

    store = _Store(yaml.safe_load(path.read_text()), match_sql=match_sql)
    core_driver.client = _Replay(store)
    try:
        with _row_source(lambda _real: _replay_factory(store)):
            yield _Cassette(path)
    finally:
        core_driver.client = previous


def _tee_factory(real, recorder):
    def factory(stream_ptr, **kwargs):
        return _TeeIterator(real(stream_ptr, **kwargs), recorder.new_row_batch())

    return factory


def _replay_factory(store):
    def factory(stream_ptr, **kwargs):
        return _Iterator([_decode(item) for item in store.next_row_batch()])

    return factory
