# Migrating from snowflake-vcrpy

`snowflake-utils[vcr]` replaces the unmaintained `snowflake-vcrpy` fork. The marker, the fixture names and the cassette layout stay the same, so most of the migration is a dependency swap plus converting existing cassettes.

## Why migrate

- `snowflake-vcrpy` is unmaintained, vendors its own copy of vcrpy and forces a `urllib3<2` pin.
- Replay was order-based and ignored request bodies, so a changed query could silently replay the wrong response.
- Request and response bodies were never scrubbed. Passwords, session tokens and account names ended up in cassettes, and the gzipped bodies hid them from grep.
- `--snowflake-vcr-mode` was declared but never read, so the mode was always vcrpy's `once`.

## Requirements

- Python 3.12 or newer.
- `snowflake-connector-python` 4.x. Connector 5.x is a different driver with no Python HTTP layer, and the plugin raises a clear error on it. Check the connector version in your lockfile before migrating.
- `urllib3` 2.x. The extra requires it, so remove any `urllib3<2` pin.

## Steps

1. **Swap the dependencies** in one commit.
   - Remove `snowflake-vcrpy`, its `[tool.uv.sources]` git entry, any `urllib3<2` pin and any separate `vcrpy` pin.
   - Add `snowflake-utils[vcr]` (in the dev group, or as the extra on the dependency you already have).
   - Move the pin to a release that ships the plugin, then re-lock only that package so the connector does not jump a major version:

     ```bash
     uv lock --upgrade-package snowflake-utils
     ```

2. **Clean up `conftest.py`.** The plugin registers the `snowflake_vcr` marker itself, so remove the manual registration and any `import snowflake.vcrpy...`. Both fixture names `snowflake_vcr_cassette` and `vcr_cassette` exist. `snowflake_vcr_config`, `snowflake_vcr_cassette_dir` and `snowflake_vcr_cassette_name` can be overridden as before.

3. **Convert the cassettes.**

   ```bash
   python -m snowflake_utils.testing.vcr convert tests/cassettes
   ```

   This gunzips and scrubs request and response bodies, drops `/session/authenticator-request`, strips signed URL parameters and canonicalizes account hostnames. It then runs the secret scan and exits non-zero if anything is left. Run your tests: re-record only the tests whose converted cassette fails replay.

4. **Check what changed in behaviour** (table below), then run the suite.

5. **Add a CI lint step** so cassettes cannot regress: `python -m snowflake_utils.testing.vcr check tests`. The test job needs no Snowflake credentials.

## Behaviour changes

| Before | Now |
| ------ | --- |
| Mode was always `once`: a missing cassette recorded silently | Default `none`: replay only, a missing cassette fails. `--snowflake-vcr-mode=rewrite` is the only way to record |
| Re-recording appended to the existing cassette | `rewrite` deletes the cassette first |
| Recording was possible in CI | `rewrite` refuses to run when `CI` is set |
| Requests matched on method, host, path and query only | Also on the SQL text of query requests, whitespace-normalized. Opt out per test with `@pytest.mark.snowflake_vcr(match_sql=False)` or in `snowflake_vcr_config` |
| Replay ran the real auth code, so it needed matching credentials | Replay uses a stub authenticator: no browser, key or password. The authenticator only matters when recording |
| The login request recorded the authenticator's traffic | One canonical login per cassette. Recording with `externalbrowser` and replaying with keypair or password works |
| Token cache could change what a recording contained | The connector's token cache is disabled while the plugin is active, so every recording starts with a full login |
| Platform detection probed cloud metadata endpoints | Disabled while the plugin is active |
| Bodies were stored gzipped and unscrubbed | Bodies are stored as readable JSON with credentials, tokens and signed URL parameters replaced by `SCRUBBED`. Recording fails if the scan finds a secret |
| OCSP cache fetches were recorded | `ocsp.snowflakecomputing.com` is not recorded. Do not assert that every recorded interaction was played |

## Recording a cassette

1. Use a dedicated, non-sensitive test database, schema and role. Response bodies hold the query results, and scrubbing hides credentials, not data.
2. Set your `SNOWFLAKE_*` variables as usual, for example `SNOWFLAKE_AUTHENTICATOR=externalbrowser` with the login name that matches your SSO identity.
3. Run `SNOWFLAKE_VCR_MODE=rewrite uv run pytest path/to/test.py::test_name`.
4. Commit the cassette. Replay it once with different credentials, or none, to confirm it stands alone.

## Notes for specific repos

**postgres-importer.** Its `tests/conftest.py` imports `snowflake.vcrpy.snowflake_vcrpy_pytest_plugin` and monkeypatches `_process_request_recording`. Delete that code. The built-in scrubbing already canonicalizes the `/results/` path and the S3 host. The `snowflake_connection` fixture carries a `@pytest.mark.snowflake_vcr` marker, which pytest ignores on fixtures: mark the tests instead.

**snowflake-setup.** It pins a much older `snowflake-utils` release, so the bump has its own checklist:

- `SNOWFLAKE_SCHEMA` is now honoured by `SnowflakeSettings`. Check CI, local and production environments before bumping.
- `connect()` logs `No Snowflake application name provided!` on every call unless `SNOWFLAKE_APPLICATION` is set.
- The connector moves from 3.x to 4.x, which changes login traffic. Confirm that `snowflake-sqlalchemy` resolves, since `alembic/env.py` builds an engine from `connect()`.
- The package now builds with hatchling. Test a fresh install from the new tag.
- 25 of its cassettes contain no login because they reuse a connection cached by an earlier test. Keep the autouse fixtures that clear that cache.

## Troubleshooting

| Symptom | Cause |
| ------- | ----- |
| `UnsupportedConnectorError` | The connector has no vendored `urllib3` (connector 5.x) or vcrpy's internals changed. The message names both versions |
| `CannotOverwriteExistingCassetteException` | No recorded interaction matched. Compare the SQL, or re-record with `--snowflake-vcr-mode=rewrite` |
| Test fails with `SQL differs` | The query changed since recording. Re-record, or set `match_sql=False` while migrating |
| `Secrets found in <cassette>` | The scan found a credential, token, signed URL parameter or account host after recording. Do not commit the cassette: report the kind named in the message |
| Empty cassette after recording | The test never reached Snowflake through the connector's HTTP layer |

## Old cassettes already in git history

Converting rewrites the files going forward. It does not remove earlier versions from git history, so any credentials or tokens those cassettes held are still in history. Treat them as exposed and rotate them if they were real.
