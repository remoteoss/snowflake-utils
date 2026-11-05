# SPIKE — cassette testing (`snowflake_utils.testing.vcr`) on connector 5.x

Repo: remoteoss/snowflake-utils @ ba3d501 (1.28.1). Spike work is uncommitted.

## Verdict
**HARD.** The seam is real and usable, but it is a *second implementation*: 5.x has no HTTP in
Python, so the vcrpy machinery cannot be reused, and result rows must be captured by teeing the
Python row iterator because the core hands Python a pointer, not row bytes. Remaining work:

1. Record once against a real account and diff replayed rows against live rows (nothing in this
   repo can prove the record path end-to-end).
2. Cover the Arrow/pandas path (`fetch_arrow_all`, `create_table_iterator`) — unrecorded.
3. Cover the async driver (`async_core_driver` is a separate singleton and code path).
4. Cover multi-statement/`nextset`, async query status, PUT/GET stream chunks, and decide the
   cassette-compatibility policy (see Risks) with docs.

## What I built

- `snowflake_utils/testing/vcr/_core_recorder.py` (new, 396 lines): record/replay of the
  protobuf RPC surface at `core_driver.client`, per-method sequence + SQL-text keying, row
  tee/replay of the row iterator, secret scrubbing, YAML cassette in a vcr-shaped envelope so
  `_scan` and `merge_cassettes` work unchanged.
- `snowflake_utils/testing/vcr/plugin.py` (+16/−5): `snowflake_vcr_cassette` dispatches to
  `_core_recorder` when the connector major is ≥5; the 4.x path is untouched.
  `_scan.py` (+13/−1): `_is_scrubbed` treats a placeholder nested inside a secret key as clean;
  4.x cassettes only carry scalars there, so its behaviour is unchanged.
- `tests/core_fake.py` (new, 134 lines): in-process fake core + canned row factory.
- `tests/test_core_recorder.py` (new, 157 lines): the 4 tests cited below.
- `spike/probe_stream_ptr.py` (new, 24 lines): reproduces the Arrow-pointer failure.

## Evidence

- VERIFIED — 5.x HTTP leaves Python: `_internal/snowflake_restful.py:139` calls
  `core_driver.connection_send_http(...)` and `_internal/api_client/bridge.py` forwards every
  call to `sf_core_python.call_proto[_async]`; 5.2.0 has no `vendored/` directory. The seam is
  injectable: `_internal/api_client/client_api.py:293-320` documents `CoreDriver.client` as
  settable and all 85 `self.client.<method>(request)` call sites take exactly one request.
- VERIFIED — connect/execute/fetch/close touch 18 distinct methods (`database_new` first),
  observed with a transport that logs then raises.
- VERIFIED — rows do not cross the seam: `_internal/statement_utils.py:82-95` requires
  `stream.value` to be an 8-byte pointer and `_internal/arrow_stream_utils.py` hands that int to
  `CythonArrowStreamTableIterator(stream_ptr, ...)`. With a null pointer `fetchall()` raises
  `RuntimeError: Stream pointer is null`.
- VERIFIED — a Python-fabricated stream pointer is unsafe: exporting a pyarrow
  `RecordBatchReader` via `_export_to_c` and passing it to that iterator kills the interpreter,
  `exit=138` (SIGBUS), reproducible with `spike/probe_stream_ptr.py`.
- VERIFIED — replay needs **no credentials**: `connect(account=..., user=...)` with no password
  succeeds against a faked core client (no auth code runs in Python).
- VERIFIED — record→replay of my own cassette passes under 5.2.0:
  `uv run --no-project --python 3.12 --with-editable . --with "snowflake-connector-python==5.2.0"
  --with "vcrpy~=9.2.0" --with pytest python -m pytest tests/test_core_recorder.py -q` → `4 passed`.
  `test_plugin_records_then_replays_offline` records in one subprocess (fake core via conftest)
  and replays in a second with no fake and no upstream at all;
  `test_record_then_replay_without_any_upstream` poisons the upstream client so any leak fails.
- VERIFIED — 4.x unchanged: `uv run --frozen pytest -q` → `173 passed, 4 skipped` (the new file
  skips itself under 4.x). Secrets: cassette holds no password or session token, and
  `_scan.scan_cassette(path) == []`.
- INFERRED — that the record path works against the real core. It is a one-line delegation
  (`getattr(real_client, name)(request)`), but the upstream has never been the Rust core here.

## What works / what does not / unknowns

Works (under 5.2.0, fake upstream): RPC record/replay with SQL-text matching, row capture,
scrubbing, scan, rewrite-mode replace/merge, replay with zero credentials in a separate process.
Does not: Arrow/pandas result path, async driver, error responses (a recorded failure is not
replayable; the rewrite flow discards the cassette), `match_sql=False` beyond one shared key.
Unknowns: whether the real core emits a stable method sequence per test (it has telemetry,
heartbeats and lazy session-parameter reads); whether any request field is client-random (would
break keying); replay behaviour for PUT/GET streams.

## Needs a live recording (only an account can confirm)

1. One `SNOWFLAKE_VCR_MODE=rewrite` run against a real account: confirms the real method set,
   that replay calls nothing unrecorded, and that the row tee sees the driver's own tuples.
2. `fetchall` beyond one chunk, plus `fetch_arrow_all` — chunk boundaries should be irrelevant
   here, but the Arrow table path is unrecorded.
3. MFA/SSO and key-pair: replay never authenticates, yet only a real recording proves the
   *record* leg still works with `mfa_callback`.
4. PUT/GET: confirm `connection_upload_stream_*`/`connection_download_stream_*` chunk shapes and
   decide whether their bytes belong in a cassette.

## Python 3.15

- VERIFIED — 4.8.1 ships wheels for cp311–cp314 (+cp314t), no cp315; 5.2.0 adds cp315
  (`curl -s https://pypi.org/pypi/snowflake-connector-python/<v>/json | jq ...`).
- VERIFIED — 4.8.1 still installs on 3.15 by building the sdist:
  `uv run --no-project --python 3.15 --with "snowflake-connector-python==4.8.1" python -c "..."`
  → `Building snowflake-connector-python==4.8.1`, 23s wall, prints `4.8.1`. With
  `--with-editable . --with "vcrpy~=9.2.0"`, `_recorder.install_patches()` also succeeds on
  3.15/4.8.1 — the 4.x path is not blocked by 3.15, it just needs a compiler.

## Risks

- `_internal` is private: the proto service, method names and `core_driver.client` can change in
  any 5.x minor. Replay fails loudly when the method sequence changes — good, but often.
- Cassettes are proto-JSON, not HTTP: they expose the RPC surface (session parameters, handles),
  are bigger, and are not portable across connector minors.
- The row tee proves "the test got these rows", not "the driver decoded this Arrow stream".
- I did **not** widen the `vcr` extra cap in `pyproject.toml`: nothing here was validated against
  a live account, and widening it changes what `pip install snowflake-utils[vcr]` resolves to.
