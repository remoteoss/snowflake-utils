# CHANGELOG

## v1.11.0 (2024-09-20)

### Feature

* feat: support oauth in snowflake utils (#14)

* feat: allow multiple authentication methods

* fix: tests, bump packages ([`6a6bcce`](https://github.com/remoteoss/snowflake-utils/commit/6a6bcce2b59ff775e5aae10109e88862af7b0858))

## v1.10.0 (2024-09-05)

### Feature

* feat: add tags at table level (#12)

* feat: add tags at column level

* chore: remove set

* feat; add enum, refactor models file

* fix: use retrocompatible typing_extensions

* feat: add settings.yml ([`1f1be19`](https://github.com/remoteoss/snowflake-utils/commit/1f1be19d4316617d8e90601f471ea8d3203c2441))

## v1.9.0 (2024-09-03)

### Feature

* feat: add qualify option to copy ([`0afa6b6`](https://github.com/remoteoss/snowflake-utils/commit/0afa6b6fae7210109e31ceee0a1e0f5ad93c4070))

## v1.8.1 (2024-08-20)

### Fix

* fix: add option to not replace chars ([`adbf254`](https://github.com/remoteoss/snowflake-utils/commit/adbf2547a9815e6672eebd7fc6b5c65546e653ca))

### Unknown

* Merge pull request #10 from remoteoss/fix-column-name-renaming

fix: add option to not replace chars ([`a558c8b`](https://github.com/remoteoss/snowflake-utils/commit/a558c8bf79cef2ebab642eea0ef3703545c43854))

* fix default value ([`a9a9157`](https://github.com/remoteoss/snowflake-utils/commit/a9a91576d7378786c560367d8f17d3b7a6dd622b))

## v1.8.0 (2024-08-08)

### Feature

* feat: allow transforming while loading in snowflake (#9)

* feat: add copy custom

* feat: add custom merge

* fix: copy custom query, quote, include_metadata not supported in copy transform

* feat: add tests

* fix: missing type annotations

* chore: remove wrong type annotation ([`5f876dc`](https://github.com/remoteoss/snowflake-utils/commit/5f876dc4424693583f2d1d3f2058f62d49d78f9b))

## v1.7.2 (2024-07-17)

### Fix

* fix: force casefold, number of arguments for unset tag ([`7d91acd`](https://github.com/remoteoss/snowflake-utils/commit/7d91acdec1d2452c8591c919d7a068c063781e1c))

## v1.7.1 (2024-07-16)

### Fix

* fix: triple quotes ([`57fa459`](https://github.com/remoteoss/snowflake-utils/commit/57fa459527a2ab84375976f9c705df5176b74d5b))

* fix: handle columns with quotes ([`52a194a`](https://github.com/remoteoss/snowflake-utils/commit/52a194ae83df20d8c200e7f94b4b8cb0823f2922))

## v1.7.0 (2024-07-16)

### Feature

* feat: update readme (#8) ([`5a56a53`](https://github.com/remoteoss/snowflake-utils/commit/5a56a532d8728ae602f4059b27557d1e4a4dd7e7))

### Unknown

* De 528 add masking policies to snowflake utils (#7)

* chore: poetry update

* feat: add support for setting tags ([`ad3b1ee`](https://github.com/remoteoss/snowflake-utils/commit/ad3b1ee6662945333d370e112998b458e13545de))

## v1.6.0 (2024-07-04)

### Feature

* feat: add bulk inserts ([`64f16d6`](https://github.com/remoteoss/snowflake-utils/commit/64f16d6d017263020640de751ac3ec07f2c4b9a3))

### Unknown

* Merge pull request #6 from remoteoss/add-bulk-inserts

feat: add bulk inserts ([`21433df`](https://github.com/remoteoss/snowflake-utils/commit/21433df6b14c66fdafed7ab00b4e481138158cd3))

* fix ruff ([`e4df22c`](https://github.com/remoteoss/snowflake-utils/commit/e4df22cdea820c680f7f1c5ebfc74d2534b4844b))

* add float type ([`5ce5db1`](https://github.com/remoteoss/snowflake-utils/commit/5ce5db1a3e2842d7d99fa8ab546273979180944a))

## v1.5.0 (2024-05-14)

### Feature

* feat: allow copying data from external storage into one blob column ([`cd2fea5`](https://github.com/remoteoss/snowflake-utils/commit/cd2fea5bb1f1eb42ef73995838f6e2530f73fbd3))

### Refactor

* refactor: formatting ([`3163096`](https://github.com/remoteoss/snowflake-utils/commit/3163096e64ed5fa7a9517970f20a4c789d0fcae4))

### Unknown

* Merge pull request #5 from remoteoss/fix-formatting

chore: run tests for prs ([`ef6c1cf`](https://github.com/remoteoss/snowflake-utils/commit/ef6c1cfe5ecef6d1433a4987a3a7e08f81ff0cde))

* Merge pull request #4 from giamo/copy-as-variant-blob

feat: allow copying data from external storage into one blob column ([`c24a0f3`](https://github.com/remoteoss/snowflake-utils/commit/c24a0f34e029f6f1e07477d2c0633eb063f89501))

## v1.4.0 (2024-04-22)

### Chore

* chore: test ([`ca657ed`](https://github.com/remoteoss/snowflake-utils/commit/ca657ed26c774f2f1e14692a00b71a419c492d86))

### Feature

* feat: add column update ([`b7b0d29`](https://github.com/remoteoss/snowflake-utils/commit/b7b0d294775d2cb24047b1b742516522f5e00c5a))

### Fix

* fix: logging level as env var ([`f4dfd30`](https://github.com/remoteoss/snowflake-utils/commit/f4dfd30073f11624b947be6b0fd5d47157ba2318))

* fix: pin python patch version ([`168c525`](https://github.com/remoteoss/snowflake-utils/commit/168c52504f29bf46966d4ffc9e2814df68d4ffff))

### Unknown

* Merge pull request #3 from remoteoss/add-column-update

feat: add column update ([`7b5478c`](https://github.com/remoteoss/snowflake-utils/commit/7b5478c185d6b8bc72eda5be6b77de1dbc3a3ecd))

* fix description ([`58f5bc6`](https://github.com/remoteoss/snowflake-utils/commit/58f5bc6290d63ab5a737b7057f47b27ef550589a))

## v1.3.1 (2024-04-17)

### Fix

* fix: version (#2)

* fix: version

* chore: test

* chore: bump dependencies

* chore: test ([`4a24607`](https://github.com/remoteoss/snowflake-utils/commit/4a2460771b79aaf73e42b0f82b6353cb5ba1dbfe))

## v1.3.0 (2024-04-16)

### Feature

* feat: add ci ([`1173377`](https://github.com/remoteoss/snowflake-utils/commit/11733778d9a18ff9a9aaad80ac4b77aee99a9b62))

* feat: first commit ([`637f9e8`](https://github.com/remoteoss/snowflake-utils/commit/637f9e8bc6837d962cc301975471b35077b7e3e3))

### Unknown

* Merge pull request #1 from remoteoss/migrate-from-gitlab

feat: first commit ([`b871cb9`](https://github.com/remoteoss/snowflake-utils/commit/b871cb9c2111c4dda3117ec44d105cc59ad0b76d))

* change version ([`c9b7ba8`](https://github.com/remoteoss/snowflake-utils/commit/c9b7ba8a68e5c4af4c0e1ab32bf2762d14244c98))

* Initial commit ([`0ac9427`](https://github.com/remoteoss/snowflake-utils/commit/0ac9427bbfc35119beed6536d11b43cb72a73812))
