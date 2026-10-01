# One-time PostgreSQL migration

Keep the source database until migration is checked.

Use this procedure to export Cloud SQL for PostgreSQL history as CSV and load
it directly into BigQuery. No intermediate tables are needed. Choose the
import procedure separately for each destination table:

- [Table already exists: Cloud Shell append](#table-already-exists).
- [Table does not exist: console creation and import](#table-does-not-exist).

## Export CSV files

Before starting, stop the old PostgreSQL collectors and confirm that the
history has not already been imported. If the old and new collectors ran in
parallel, restrict the export queries to the intended historical period per
account with a WHERE clause to avoid overlapping observations.

1. Create a Cloud Storage bucket in the same location as the BigQuery dataset.
   Grant the Cloud SQL instance service account write access to that bucket.
   The operator needs Cloud SQL export permission, bucket read access, and
   BigQuery job creation and table create/write permissions. Create the
   destination dataset first if it does not exist.
2. In Cloud SQL, open the instance and select **Export**. Choose **CSV**, the
   source database, and the destination object in Cloud Storage. Run each SQL
   query below as a separate export. Replace `public` if the source tables use
   a different schema.

For `hist_positions.csv`:

```sql
SELECT
  account, symbol, size, mark_price,
  to_char(
    to_timestamp(fetched_at / 1000.0) AT TIME ZONE 'UTC',
    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
  ) AS fetched_at
FROM public.hist_positions;
```

For `hist_collaterals.csv`:

```sql
SELECT
  account, currency, collateral, collateral_jpy, collateral_usd,
  to_char(
    to_timestamp(fetched_at / 1000.0) AT TIME ZONE 'UTC',
    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
  ) AS fetched_at
FROM public.hist_collaterals;
```

These queries omit the old id column, preserve the destination column order,
and convert Unix milliseconds to UTC timestamp strings.

## Table already exists

Open **Cloud Shell** in the Google Cloud console and run the command for each
existing table. Replace `PROJECT` and `DATASET` with the collector's
CRYPTO_SYNC_BQ_PROJECT and CRYPTO_SYNC_BQ_DATASET values, and `BUCKET` with the
export bucket name. Adjust the object paths if the CSV files are in a folder.

```sh
bq load --source_format=CSV --autodetect=false --noreplace \
  --skip_leading_rows=0 \
  PROJECT:DATASET.hist_positions \
  gs://BUCKET/hist_positions.csv

bq load --source_format=CSV --autodetect=false --noreplace \
  --skip_leading_rows=0 \
  PROJECT:DATASET.hist_collaterals \
  gs://BUCKET/hist_collaterals.csv
```

No schema re-entry is needed: with auto-detection disabled and no schema
argument, `bq load` uses the existing destination table's schema. CSV columns
match by position. `--noreplace` appends rows without overwriting existing
data. If a CSV has a column-name header, use `--skip_leading_rows=1` for that
file instead. The default number of allowed bad records is zero.

Do not use the BigQuery **Create table** form for this step. The documented
console procedure asks for a schema and does not support load-job appends to
partitioned or clustered tables; this application's tables use both.
See the [official append instructions](https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#appending_to_or_overwriting_a_table_with_csv_data).

## Table does not exist

Keep the BigQuery collector stopped until both tables have been created and
the import checked, so it does not create the tables during this procedure.

1. In BigQuery, open the destination dataset and select **Create table**.
   Choose **Google Cloud Storage** as the source, select the corresponding
   CSV, and set the file format to **CSV**. Set the destination project and
   dataset to the collector's CRYPTO_SYNC_BQ_PROJECT and CRYPTO_SYNC_BQ_DATASET.
   Name the table `hist_positions` or `hist_collaterals` to match the file.
2. Disable schema auto-detection. Select **Edit as text** and paste the
   matching JSON schema below. The field order and REQUIRED/NULLABLE modes
   match the application; auto-detection may produce incompatible modes.

For `hist_positions`:

```json
[
  {"name": "account", "type": "STRING", "mode": "REQUIRED"},
  {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
  {"name": "size", "type": "FLOAT", "mode": "REQUIRED"},
  {"name": "mark_price", "type": "FLOAT", "mode": "REQUIRED"},
  {"name": "fetched_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
]
```

For `hist_collaterals`:

```json
[
  {"name": "account", "type": "STRING", "mode": "REQUIRED"},
  {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
  {"name": "collateral", "type": "FLOAT", "mode": "REQUIRED"},
  {"name": "collateral_jpy", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "collateral_usd", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "fetched_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
]
```

3. Set the following options, then select **Create table**. Repeat for the
   other missing table. This creates each table and loads its CSV in one job.

| Setting | Value |
| --- | --- |
| Partitioning | By field: fetched_at; partition type: Day |
| Clustering | account, symbol for hist_positions; account for hist_collaterals |
| Write preference | Write if empty; never Overwrite table |
| Source column match | Position |
| Header rows to skip | 0 without a header; 1 with a column-name header |
| Number of errors allowed | 0 |

The application requires daily fetched_at partitioning and the exact schema
above. Do not also run the append commands for a CSV successfully loaded here.
See the [official new table instructions](https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_into_a_table).

## Verify the import

Confirm each load job succeeded. Check row counts for the imported period
and accounts against the source, and inspect historical timestamps and
nullable collateral values. Do not reload a successful CSV: append does
not deduplicate. If a job's outcome is unclear, check its status before
retrying. Keep existing production tables intact if a load fails.

References: [Cloud SQL CSV export](https://docs.cloud.google.com/sql/docs/postgres/import-export/import-export-csv),
[BigQuery CSV load](https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv),
[PostgreSQL timestamp formatting](https://www.postgresql.org/docs/current/functions-formatting.html).
