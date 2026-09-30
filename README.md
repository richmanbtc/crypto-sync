# crypto-sync

Collect exchange positions and collateral into BigQuery. One process handles
one account. PostgreSQL is only a source for the optional one-time migration;
there is no PostgreSQL output mode. Dashboards are outside this repository.

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
collection or migration; incompatible tables cause startup to fail. Use Google
Application Default Credentials (ADC): an attached workload identity or an
externally provided credential mechanism. Do not bake credential files into
the image. The container runs as root.

The identity needs table create/read/write access in the destination dataset
and permission to create query/load jobs in the project. Typical roles are
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

Stop all old collectors, import history, then start the BigQuery collectors.
The downtime gap is accepted. Keep the source PostgreSQL database and the old
image until the migration is checked. All accounts in the two history tables
are imported, not just CRYPTO_SYNC_ACCOUNT.

When running from source, install into a Python 3.12 environment:

```sh
python -m pip install -r requirements.txt
python -m src.migrate_postgres --batch-size 100000
```

Supply CRYPTO_SYNC_DATABASE_URL for the source plus the BigQuery settings and
ADC described above. Exchange credentials are not needed. Never put the source
connection string into command-line arguments or commit it. The collector
image includes psycopg2, so no additional migration dependencies are needed.
In the built image, run the migration module directly; skip the pip commands.

The command uses read-only PostgreSQL transactions and a server-side cursor.
It loads at most batch-size rows at a time, waits for each append load job,
and logs confirmed row counts. Upload retries are disabled. Any error stops
the command with exit status 1. There is no checkpoint, resume, deduplication,
or automatic cleanup. The default batch size limits the number of load jobs;
adjust it for available memory and dataset size.

Run against empty destination tables. If it fails, manually delete both
hist_positions and hist_collaterals, then rerun from the beginning. A timed-out
load job might still be running: inspect/cancel it and wait for it to finish
before deleting tables and starting again. Do this before starting the live
collector, since deleting the tables also deletes newly collected data.
Do not rerun a successful migration into the same tables: that appends duplicates.

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
cycles, migration batching/abort, configuration, and watchdogs.

Requirements include psycopg2 for the migration command; SQLAlchemy is unused. CCXT stays at production version 4.2.1. Python
3.12.14 and the base image digest remain pinned; dependency updates require
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
