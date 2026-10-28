"""pytest plugin; loaded for every install, so only pytest and the stdlib at import."""

import os
from contextlib import ExitStack
from pathlib import Path

import pytest

MODES = ("none", "rewrite")
SELECTIONS = ("annotated", "all", "disable")
MODE_ENV = "SNOWFLAKE_VCR_MODE"


def pytest_addoption(parser):
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


@pytest.fixture
def snowflake_vcr_cassette(
    request,
    snowflake_vcr_config,
    snowflake_vcr_cassette_dir,
    snowflake_vcr_cassette_name,
):
    from ._recorder import build_vcr, install_patches

    marker = request.node.get_closest_marker("snowflake_vcr")
    kwargs = {**snowflake_vcr_config, **(marker.kwargs if marker else {})}
    match_sql = kwargs.pop("match_sql", True)
    name = kwargs.pop("path", snowflake_vcr_cassette_name)
    rewrite = _mode(request.config) == "rewrite"
    recorder = build_vcr(
        cassette_dir=snowflake_vcr_cassette_dir,
        match_sql=match_sql,
        **{"record_mode": "all" if rewrite else "none", **kwargs},
    )
    install_patches()
    with ExitStack() as stack:
        from ._recorder import connector_overrides

        stack.enter_context(connector_overrides(replay=not rewrite))
        if rewrite:
            filename = name if name.endswith(".yaml") else f"{name}.yaml"
            Path(snowflake_vcr_cassette_dir, filename).unlink(missing_ok=True)
        yield stack.enter_context(recorder.use_cassette(name))


@pytest.fixture
def vcr_cassette(snowflake_vcr_cassette):
    return snowflake_vcr_cassette


@pytest.fixture(autouse=True)
def _snowflake_vcr_marker(request):
    selection = request.config.getoption("snowflake_record_tests_selection")
    marked = request.node.get_closest_marker("snowflake_vcr") is not None
    if selection == "all" or (selection == "annotated" and marked):
        request.getfixturevalue("snowflake_vcr_cassette")
