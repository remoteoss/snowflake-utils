# CHANGELOG


## v1.14.0 (2025-01-10)

### Features

- Update typer
  ([`b087cf0`](https://github.com/remoteoss/snowflake-utils/commit/b087cf09324e876de458bdb571c6f19a4088c2f6))


## v1.13.0 (2025-01-08)

### Features

- Qualify only on sync tags true ([#16](https://github.com/remoteoss/snowflake-utils/pull/16),
  [`8179f5b`](https://github.com/remoteoss/snowflake-utils/commit/8179f5b2735bd9c4b15dc46c91a1ac6eee1af59d))


## v1.12.0 (2024-12-05)

### Features

- Reuse connections
  ([`fc0139f`](https://github.com/remoteoss/snowflake-utils/commit/fc0139f752b2ec74f307cce78f15c10000b865a6))


## v1.11.2 (2024-10-08)

### Bug Fixes

- Always quote partition by ([#15](https://github.com/remoteoss/snowflake-utils/pull/15),
  [`0455397`](https://github.com/remoteoss/snowflake-utils/commit/045539795f358cfe64bf58246b2efa7f028953f2))


## v1.11.1 (2024-09-25)

### Bug Fixes

- Only set tags on columns that exist
  ([`d12836e`](https://github.com/remoteoss/snowflake-utils/commit/d12836e8908c33d7d5680aee23ae6689ce4e5245))


## v1.11.0 (2024-09-20)

### Features

- Support oauth in snowflake utils ([#14](https://github.com/remoteoss/snowflake-utils/pull/14),
  [`6a6bcce`](https://github.com/remoteoss/snowflake-utils/commit/6a6bcce2b59ff775e5aae10109e88862af7b0858))

* feat: allow multiple authentication methods

* fix: tests, bump packages


## v1.10.0 (2024-09-05)

### Features

- Add tags at table level ([#12](https://github.com/remoteoss/snowflake-utils/pull/12),
  [`1f1be19`](https://github.com/remoteoss/snowflake-utils/commit/1f1be19d4316617d8e90601f471ea8d3203c2441))

* feat: add tags at column level

* chore: remove set

* feat; add enum, refactor models file

* fix: use retrocompatible typing_extensions

* feat: add settings.yml


## v1.9.0 (2024-09-03)

### Features

- Add qualify option to copy
  ([`0afa6b6`](https://github.com/remoteoss/snowflake-utils/commit/0afa6b6fae7210109e31ceee0a1e0f5ad93c4070))


## v1.8.1 (2024-08-20)

### Bug Fixes

- Add option to not replace chars
  ([`adbf254`](https://github.com/remoteoss/snowflake-utils/commit/adbf2547a9815e6672eebd7fc6b5c65546e653ca))


## v1.8.0 (2024-08-08)

### Features

- Allow transforming while loading in snowflake
  ([#9](https://github.com/remoteoss/snowflake-utils/pull/9),
  [`5f876dc`](https://github.com/remoteoss/snowflake-utils/commit/5f876dc4424693583f2d1d3f2058f62d49d78f9b))

* feat: add copy custom

* feat: add custom merge

* fix: copy custom query, quote, include_metadata not supported in copy transform

* feat: add tests

* fix: missing type annotations

* chore: remove wrong type annotation


## v1.7.2 (2024-07-17)

### Bug Fixes

- Force casefold, number of arguments for unset tag
  ([`7d91acd`](https://github.com/remoteoss/snowflake-utils/commit/7d91acdec1d2452c8591c919d7a068c063781e1c))


## v1.7.1 (2024-07-16)

### Bug Fixes

- Handle columns with quotes
  ([`52a194a`](https://github.com/remoteoss/snowflake-utils/commit/52a194ae83df20d8c200e7f94b4b8cb0823f2922))

- Triple quotes
  ([`57fa459`](https://github.com/remoteoss/snowflake-utils/commit/57fa459527a2ab84375976f9c705df5176b74d5b))


## v1.7.0 (2024-07-16)

### Features

- Update readme ([#8](https://github.com/remoteoss/snowflake-utils/pull/8),
  [`5a56a53`](https://github.com/remoteoss/snowflake-utils/commit/5a56a532d8728ae602f4059b27557d1e4a4dd7e7))


## v1.6.0 (2024-07-04)

### Features

- Add bulk inserts
  ([`64f16d6`](https://github.com/remoteoss/snowflake-utils/commit/64f16d6d017263020640de751ac3ec07f2c4b9a3))


## v1.5.0 (2024-05-14)

### Features

- Allow copying data from external storage into one blob column
  ([`cd2fea5`](https://github.com/remoteoss/snowflake-utils/commit/cd2fea5bb1f1eb42ef73995838f6e2530f73fbd3))

### Refactoring

- Formatting
  ([`3163096`](https://github.com/remoteoss/snowflake-utils/commit/3163096e64ed5fa7a9517970f20a4c789d0fcae4))


## v1.4.0 (2024-04-22)

### Bug Fixes

- Logging level as env var
  ([`f4dfd30`](https://github.com/remoteoss/snowflake-utils/commit/f4dfd30073f11624b947be6b0fd5d47157ba2318))

- Pin python patch version
  ([`168c525`](https://github.com/remoteoss/snowflake-utils/commit/168c52504f29bf46966d4ffc9e2814df68d4ffff))

### Chores

- Test
  ([`ca657ed`](https://github.com/remoteoss/snowflake-utils/commit/ca657ed26c774f2f1e14692a00b71a419c492d86))

### Features

- Add column update
  ([`b7b0d29`](https://github.com/remoteoss/snowflake-utils/commit/b7b0d294775d2cb24047b1b742516522f5e00c5a))


## v1.3.1 (2024-04-17)

### Bug Fixes

- Version ([#2](https://github.com/remoteoss/snowflake-utils/pull/2),
  [`4a24607`](https://github.com/remoteoss/snowflake-utils/commit/4a2460771b79aaf73e42b0f82b6353cb5ba1dbfe))

* fix: version

* chore: test

* chore: bump dependencies


## v1.3.0 (2024-04-16)

### Features

- Add ci
  ([`1173377`](https://github.com/remoteoss/snowflake-utils/commit/11733778d9a18ff9a9aaad80ac4b77aee99a9b62))

- First commit
  ([`637f9e8`](https://github.com/remoteoss/snowflake-utils/commit/637f9e8bc6837d962cc301975471b35077b7e3e3))
