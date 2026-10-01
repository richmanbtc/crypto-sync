"""BigQuery history storage. Failed writes are never resubmitted."""

from datetime import datetime, timedelta, timezone
import re

from google.cloud import bigquery

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DAY_MS = 24 * 60 * 60 * 1000


def fields(*items):
    return [bigquery.SchemaField(name, kind, mode=mode) for name, kind, mode in items]


SCHEMAS = {
    'hist_positions': fields(
        ('account', 'STRING', 'REQUIRED'), ('symbol', 'STRING', 'REQUIRED'),
        ('size', 'FLOAT64', 'REQUIRED'), ('mark_price', 'FLOAT64', 'REQUIRED'),
        ('fetched_at', 'TIMESTAMP', 'REQUIRED'),
    ),
    'hist_collaterals': fields(
        ('account', 'STRING', 'REQUIRED'), ('currency', 'STRING', 'REQUIRED'),
        ('collateral', 'FLOAT64', 'REQUIRED'),
        ('collateral_jpy', 'FLOAT64', 'NULLABLE'),
        ('collateral_usd', 'FLOAT64', 'NULLABLE'),
        ('fetched_at', 'TIMESTAMP', 'REQUIRED'),
    ),
}


def timestamp(milliseconds):
    return EPOCH + timedelta(milliseconds=int(milliseconds))


def encode_rows(table, rows):
    result = []
    for row in rows:
        converted = {field.name: row[field.name] if field.mode == 'REQUIRED'
                     else row.get(field.name) for field in SCHEMAS[table]}
        converted['fetched_at'] = timestamp(row['fetched_at']).isoformat()
        result.append(converted)
    return result


class BigQueryStore:
    def __init__(self, client, dataset):
        # These identifiers appear in SQL; reject quoting and SQL metacharacters.
        if not re.fullmatch(r'[A-Za-z0-9_.:-]+', client.project):
            raise ValueError('Invalid BigQuery project identifier')
        if not re.fullmatch(r'[A-Za-z0-9_]+', dataset):
            raise ValueError('Invalid BigQuery dataset identifier')
        self.client = client
        self.dataset = f'{client.project}.{dataset}'

    def table_id(self, name):
        if name not in SCHEMAS:
            raise ValueError('Unknown history table')
        return f'{self.dataset}.{name}'

    def ensure_tables(self):
        # The dataset must already exist; never create datasets or delete tables.
        for name, schema in SCHEMAS.items():
            table = bigquery.Table(self.table_id(name), schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(field='fetched_at')
            table.clustering_fields = ['account', 'symbol'] if name == 'hist_positions' else ['account']
            existing = self.client.create_table(table, exists_ok=True, retry=None, timeout=30)
            self._validate_table(name, existing)

    @staticmethod
    def _validate_table(name, table):
        # The API may return FLOAT for a schema declared with the FLOAT64 alias.
        def signature(field):
            kind = 'FLOAT' if field.field_type == 'FLOAT64' else field.field_type
            return kind, field.mode

        actual = {field.name: signature(field) for field in table.schema}
        expected = {field.name: signature(field) for field in SCHEMAS[name]}
        if actual != expected:
            raise ValueError(f'Incompatible schema for {name}')
        partition = table.time_partitioning
        if partition is None or partition.field != 'fetched_at' or partition.type_ != 'DAY':
            raise ValueError(f'Incompatible partitioning for {name}')

    def recent_symbols(self, account, since):
        config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter('account', 'STRING', account),
            bigquery.ScalarQueryParameter('since', 'TIMESTAMP', timestamp(since)),
        ])
        job = self.client.query(
            f'''SELECT symbol, MAX(UNIX_MILLIS(fetched_at)) AS last_seen
                FROM `{self.table_id('hist_positions')}`
                WHERE account = @account AND size != 0 AND fetched_at >= @since
                GROUP BY symbol''',
            job_config=config, retry=None, job_retry=None, timeout=30,
        )
        return {row['symbol']: row['last_seen'] for row in
                job.result(retry=None, job_retry=None, timeout=60)}

    def _insert(self, table, rows):
        if not rows:
            return
        errors = self.client.insert_rows_json(
            self.table_id(table), encode_rows(table, rows),
            row_ids=bigquery.AutoRowIDs.DISABLED, retry=None, timeout=30,
        )
        if errors:
            # Do not log response bodies, row contents, or credential-bearing errors.
            raise RuntimeError(f'BigQuery rejected {len(errors)} rows')

    def insert_positions(self, rows):
        self._insert('hist_positions', rows)

    def insert_collateral(self, row):
        self._insert('hist_collaterals', [row])
