import itertools
import os
import types
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from unittest import mock

from ._scrub import (
    FILTERED_QUERY_PARAMS,
    IGNORED_HOSTS,
    before_record_request,
    before_record_response,
    sql_matcher,
)

_AUTH_KWARGS = (
    "authenticator",
    "password",
    "token",
    "private_key",
    "private_key_file",
    "private_key_file_pwd",
    "private_key_password",
)
_installed = False


class UnsupportedConnectorError(RuntimeError):
    pass


def _connector_version() -> str:
    try:
        return version("snowflake-connector-python")
    except PackageNotFoundError:
        return "not installed"


def _unsupported(reason: str) -> UnsupportedConnectorError:
    return UnsupportedConnectorError(
        f"snowflake_utils.testing.vcr supports snowflake-connector-python 4.x "
        f"(found {_connector_version()}, vcrpy {_vcrpy_version()}): {reason}"
    )


def _vcrpy_version() -> str:
    try:
        return version("vcrpy")
    except PackageNotFoundError:
        return "not installed"


def install_patches() -> None:
    """Make vcrpy intercept the connector's vendored urllib3, once per process."""
    global _installed
    if _installed:
        return
    try:
        import snowflake.connector.vendored.urllib3.connection as sf_conn
        import snowflake.connector.vendored.urllib3.connectionpool as sf_cpool
    except ImportError as exc:
        raise _unsupported("the connector has no vendored urllib3") from exc
    try:
        from vcr.patch import CassettePatcherBuilder
        from vcr.stubs import VCRHTTPConnection, VCRHTTPSConnection
        from vcr.stubs.urllib3_stubs import _VCRRequestsConnectionMixin

        urllib3_patchers = CassettePatcherBuilder._urllib3_patchers
    except (ImportError, AttributeError) as exc:
        raise _unsupported("vcrpy internals changed") from exc

    class HTTPConnection(
        _VCRRequestsConnectionMixin, VCRHTTPConnection, sf_conn.HTTPConnection
    ):
        _baseclass = sf_conn.HTTPConnection
        is_multiplexed = False

    class HTTPSConnection(
        _VCRRequestsConnectionMixin, VCRHTTPSConnection, sf_conn.VerifiedHTTPSConnection
    ):
        _baseclass = sf_conn.VerifiedHTTPSConnection
        is_multiplexed = False

    stubs = types.SimpleNamespace(
        VCRRequestsHTTPConnection=HTTPConnection,
        VCRRequestsHTTPSConnection=HTTPSConnection,
    )
    build = CassettePatcherBuilder.build
    CassettePatcherBuilder.build = lambda self: itertools.chain(
        build(self), urllib3_patchers(self, sf_cpool, sf_conn, stubs)
    )
    _installed = True


def _stub_auth():
    from snowflake.connector.auth import AuthByKeyPair

    class StubAuth(AuthByKeyPair):
        def __init__(self):
            super().__init__(private_key=b"unused")

        def prepare(self, **kwargs):
            return None

        def update_body(self, body):
            body["data"]["AUTHENTICATOR"] = "SNOWFLAKE_JWT"
            body["data"]["TOKEN"] = "STUB"

    return StubAuth()


class _NoopTokenCache:
    def store(self, *args, **kwargs):
        return None

    def retrieve(self, *args, **kwargs):
        return None

    def remove(self, *args, **kwargs):
        return None


@contextmanager
def connector_overrides(*, replay: bool):
    """Keep recordings reproducible and let replay authenticate without secrets."""
    from snowflake.connector import connection, token_cache

    noop = getattr(token_cache, "NoopTokenCache", _NoopTokenCache)
    patches = [
        mock.patch.object(
            token_cache.TokenCache, "make", staticmethod(lambda *a, **k: noop())
        ),
        mock.patch.dict(os.environ, {"SNOWFLAKE_DISABLE_PLATFORM_DETECTION": "true"}),
    ]
    if replay:
        original_init = connection.SnowflakeConnection.__init__

        def init(self, *args, **kwargs):
            for key in _AUTH_KWARGS:
                kwargs.pop(key, None)
            original_init(self, *args, auth_class=_stub_auth(), **kwargs)

        patches.append(
            mock.patch.object(connection.SnowflakeConnection, "__init__", init)
        )
    for patch in patches:
        patch.start()
    try:
        yield
    finally:
        for patch in reversed(patches):
            patch.stop()


def build_vcr(*, cassette_dir: str, record_mode: str, match_sql: bool = True, **extra):
    import vcr

    config = {
        "cassette_library_dir": cassette_dir,
        "record_mode": record_mode,
        "match_on": ["method", "scheme", "host", "port", "path"]
        + (["sql"] if match_sql else []),
        "filter_query_parameters": FILTERED_QUERY_PARAMS,
        "before_record_request": before_record_request,
        "before_record_response": before_record_response,
        "decode_compressed_response": True,
        "ignore_hosts": IGNORED_HOSTS,
        "path_transformer": vcr.VCR.ensure_suffix(".yaml"),
        **extra,
    }
    recorder = vcr.VCR(**config)
    recorder.register_matcher("sql", sql_matcher)
    return recorder
