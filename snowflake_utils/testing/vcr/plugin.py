"""pytest plugin; loaded for every install, so only pytest and the stdlib at import."""

import importlib.util
import os
from contextlib import ExitStack
from pathlib import Path

import pytest

MODES = ("none", "rewrite")
SELECTIONS = ("annotated", "all", "disable")
MODE_ENV = "SNOWFLAKE_VCR_MODE"


def _legacy_plugin_installed() -> bool:
    try:
        return importlib.util.find_spec("snowflake.vcrpy") is not None
    except (ImportError, ValueError):
        return False


def pytest_addoption(parser):
    if _legacy_plugin_installed():
        return  # registering the same options would crash pytest; configure reports it
    group = parser.getgroup("snowflake-vcr")
    group.addoption(
        "--snowflake-vcr-mode",
        dest="snowflake_vcr_mode",
        choices=MODES,
        default=None,
        help=f"none: replay only (default). rewrite: re-record each cassette. "
        f"Env: {MODE_ENV}.",
    )
    group.addoption(
        "--snowflake-record-tests-selection",
        dest="snowflake_record_tests_selection",
        choices=SELECTIONS,
        default="annotated",
        help="Which tests get a cassette: only marked ones, all, or none.",
    )


def _mode(config) -> str:
    mode = config.getoption("snowflake_vcr_mode") or os.environ.get(MODE_ENV, "none")
    if mode not in MODES:
        raise pytest.UsageError(f"{MODE_ENV} must be one of {MODES}, got {mode!r}")
    return mode


def pytest_configure(config):
    if _legacy_plugin_installed():
        raise pytest.UsageError(
            "snowflake-vcrpy is installed and conflicts with snowflake-utils[vcr]. "
            "Uninstall it: see docs/migrating-from-snowflake-vcrpy.md"
        )
    config.addinivalue_line(
        "markers", "snowflake_vcr: Mark the test as using Snowflake VCR."
    )
    if _mode(config) == "rewrite" and os.environ.get("CI"):
        raise pytest.UsageError("Refusing to record Snowflake cassettes when CI is set")


@pytest.fixture(scope="module")
def snowflake_vcr_config() -> dict:
    return {}


@pytest.fixture(scope="module")
def snowflake_vcr_cassette_dir(request) -> str:
    return str(Path(request.node.path).parent / "cassettes")


@pytest.fixture
def snowflake_vcr_cassette_name(request) -> str:
    if request.cls is not None:
        return f"{request.cls.__name__}.{request.node.name}"
    return request.node.name


def _check_record_mode(kwargs: dict) -> None:
    requested = kwargs.pop("record_mode", "none")
    requested = getattr(requested, "value", requested)
    if requested != "none":
        pytest.fail(
            f"record_mode={requested!r} is not supported: "
            f"record with --snowflake-vcr-mode=rewrite",
            pytrace=False,
        )


@pytest.fixture(scope="module")
def snowflake_vcr(request, snowflake_vcr_config, snowflake_vcr_cassette_dir):
    from ._recorder import build_vcr

    kwargs = dict(snowflake_vcr_config)
    _check_record_mode(kwargs)
    match_sql = kwargs.pop("match_sql", True)
    rewrite = _mode(request.config) == "rewrite"
    return build_vcr(
        cassette_dir=snowflake_vcr_cassette_dir,
        record_mode="all" if rewrite else "none",
        match_sql=match_sql,
        **kwargs,
    )


@pytest.fixture
def snowflake_vcr_cassette(
    request,
    snowflake_vcr_config,
    snowflake_vcr_cassette_dir,
    snowflake_vcr_cassette_name,
):
    from ._recorder import build_vcr, connector_overrides, install_patches

    marker = request.node.get_closest_marker("snowflake_vcr")
    kwargs = {**snowflake_vcr_config, **(marker.kwargs if marker else {})}
    _check_record_mode(kwargs)
    match_sql = kwargs.pop("match_sql", True)
    name = kwargs.pop("path", snowflake_vcr_cassette_name)
    rewrite = _mode(request.config) == "rewrite"
    cassette_root = Path(snowflake_vcr_cassette_dir).resolve()
    filename = name if name.endswith(".yaml") else f"{name}.yaml"
    path = (cassette_root / filename).resolve()
    if cassette_root not in path.parents:
        pytest.fail(f"Cassette {name!r} is outside {cassette_root}", pytrace=False)
    recorder = build_vcr(
        cassette_dir=snowflake_vcr_cassette_dir,
        record_mode="all" if rewrite else "none",
        match_sql=match_sql,
        **kwargs,
    )
    install_patches()
    with ExitStack() as stack:
        stack.enter_context(connector_overrides(replay=not rewrite))
        if rewrite:
            path.unlink(missing_ok=True)
        yield stack.enter_context(recorder.use_cassette(name))
    if rewrite and path.exists():
        from ._scan import scan_cassette

        kinds = sorted({finding.kind for finding in scan_cassette(path)})
        if kinds:
            pytest.fail(
                f"Secrets found in {path.name}: {', '.join(kinds)}", pytrace=False
            )


@pytest.fixture
def vcr_cassette(snowflake_vcr_cassette):
    return snowflake_vcr_cassette


@pytest.fixture(autouse=True)
def _snowflake_vcr_marker(request):
    selection = request.config.getoption("snowflake_record_tests_selection")
    marked = request.node.get_closest_marker("snowflake_vcr") is not None
    if selection == "all" or (selection == "annotated" and marked):
        request.getfixturevalue("snowflake_vcr_cassette")
