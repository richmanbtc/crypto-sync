# crypto-sync

Collect exchange positions and collateral into BigQuery. One process handles
one account. Historical PostgreSQL data can be imported manually using the
linked migration guide. Dashboards are outside this repository.

## Operation

Each cycle fetches positions and collateral, writes them sequentially using
BigQuery insertAll, then waits 60 seconds. The existing 2-second delay before
exchange requests remains. There is no local queue, deduplication, or replay.
Write requests use retry=None and no insert IDs. A failed request or row-level
error ends that cycle; its remaining data is discarded. The next cycle fetches
new data. A request that lost its response may already have been accepted by
BigQuery, and partially accepted batches remain stored. No reconciliation runs.

At startup, the process queries the account's last 24 hours of nonzero positions
once. It then tracks each symbol's last nonzero observation in memory, expiring
entries after 24 hours. This cache tracks observations even if a write fails.
Returned zero positions are kept only for recent symbols. Positions omitted by
an exchange are not synthesized. Long and short positions remain netted.

Only a fully successful cycle pings the watchdog. Continuous failures still
exit with status 1 after the configured deadline; the deployment controls
restarts. SIGTERM exits immediately, including during a cycle. There is no
shutdown flush or cleanup sequence; the watchdog runs as a daemon thread.
Logs report exception classes and stack locations, not API response bodies,
credentials, account balances, or connection strings.

## BigQuery setup

Create the production dataset outside this application and enable BigQuery.
The collector creates hist_positions and hist_collaterals if absent. It does
not create datasets, delete tables, or change existing schemas. Existing column
types, nullability, and daily fetched_at partitioning are checked before
collection; incompatible tables cause startup to fail. Use Google
Application Default Credentials (ADC): an attached workload identity or an
externally provided credential mechanism. Do not bake credential files into
the image. The container runs as root.

The identity needs table create/read/write access in the destination dataset
and permission to create query jobs in the project. Typical roles are
BigQuery Data Editor scoped to the dataset and BigQuery Job User on the project.
Use read-only exchange API keys; no trading or withdrawal permissions are needed.

| Variable | Meaning |
| --- | --- |
| CCXT_EXCHANGE | Required exchange identifier |
| CCXT_API_KEY | Exchange API key |
| CCXT_API_SECRET | Exchange API secret |
| CCXT_API_PASSWORD | Exchange password if required |
| CRYPTO_SYNC_ACCOUNT | Required account label |
| CRYPTO_SYNC_ACCOUNT_TYPE | Empty by default; Bybit also accepts btc, eth, unified |
| CRYPTO_SYNC_BQ_PROJECT | Required Google Cloud project |
| CRYPTO_SYNC_BQ_DATASET | Required dataset ID within that project |
| CRYPTO_SYNC_BQ_LOCATION | Dataset location; optional when inferred by BigQuery |
| CRYPTO_SYNC_LOG_LEVEL | INFO by default |
| CRYPTO_SYNC_PANIC_INTERVAL | Positive seconds; default 300 |

```sh
python -m src.main
```

Authentication is supplied by the runtime environment. Deployment configuration
is maintained outside this repository.

## Tables

Both tables are partitioned daily on fetched_at. hist_positions is clustered
by account and symbol; hist_collaterals by account. No expiration is configured.

| Table | Columns |
| --- | --- |
| hist_positions | account STRING, symbol STRING, size FLOAT64, mark_price FLOAT64, fetched_at TIMESTAMP |
| hist_collaterals | account STRING, currency STRING, collateral FLOAT64, collateral_jpy FLOAT64, collateral_usd FLOAT64, fetched_at TIMESTAMP |

Only collateral_jpy and collateral_usd are nullable. fetched_at is now a UTC
TIMESTAMP, converted from the old Unix milliseconds without losing millisecond
precision. PostgreSQL's internal id column is not copied. Orders are not
collected or migrated. There are no uniqueness constraints or deduplication.

Streaming is used for the small live batches so data can become queryable
without accumulating multiple cycles. Streaming charges and availability are
subject to BigQuery's service behavior; a 5-10 minute delay is acceptable for
this collector. Historical imports use load jobs rather than streaming, which
also allows old dates to be loaded into time-partitioned tables.

## One-time PostgreSQL migration

See the [PostgreSQL to BigQuery migration guide](docs/postgres-to-bigquery.md).

## Tests and dependencies

```sh
python -m pip install -r requirements.txt
python -m pip check
python -m unittest discover -v
```

Fixtures are synthetic. Tests block sockets and DNS; no development dataset,
Google credentials, production database, or exchange API is used. Tests cover
exchange normalization/signing, real BigQuery SDK request serialization and
no-retry behavior, row errors, timestamps, the recent-symbol cache, discarded
cycles, configuration, and watchdogs.

CCXT stays at production version 4.2.1. Python 3.12.14 and the base image
digest remain pinned; dependency updates require
fresh installation and tests.

## Release checks

```sh
docker build --pull --tag crypto-sync:checked .
```

This requires Docker and network access. The build runs offline tests; a build
or test failure blocks publication. CI does not run vulnerability scans.
The publication workflow publishes that same tested image without rebuilding.
Deploy by image digest; update the base image digest intentionally. Python
versions are pinned without artifact hashes. The runtime runs as root and
excludes tests.

Pip is supplied by the digest-pinned Python base image. Build dependencies
are installed automatically by pip when needed and are not pinned. No vulnerability exceptions
are configured. The current base image includes pip 25.0.1, for which Trivy
reported six vulnerabilities on 2026-09-30 (CVE-2025-8869, CVE-2026-13346,
CVE-2026-3219, CVE-2026-6357, CVE-2026-8643, CVE-2026-1703).
These findings are not resolved by disabling CI scans.

Python-only auditing is possible without Docker from a separate tooling env:

```sh
pip-audit --no-deps --disable-pip -r requirements.txt
```

Also verify dependency closure via a clean installation and pip check. An audit
is point-in-time evidence of known advisories, not a safety guarantee. No live
BigQuery/PostgreSQL access or Docker image scan was performed during this change.
Initial production startup remains the authentication/permissions/schema check.

References:

- https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client
- https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
- https://docs.cloud.google.com/bigquery/docs/authentication
