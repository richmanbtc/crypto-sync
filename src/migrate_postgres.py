"""One-shot PostgreSQL history import. Run before starting the collector."""

import argparse
import os

from google.cloud import bigquery

from .logger import create_logger, failure_location
from .storage import BigQueryStore, SCHEMAS


def migrate(connection, store, logger, batch_size=100000):
    if batch_size <= 0:
        raise ValueError('batch_size must be positive')
    connection.set_session(readonly=True)
    counts = {}
    for table, schema in SCHEMAS.items():
        columns = [field.name for field in schema]
        count = 0
        with connection.cursor(name=f'migrate_{table}') as cursor:
            cursor.itersize = batch_size
            # Identifiers come only from the fixed application schema.
            cursor.execute(f"SELECT {', '.join(columns)} FROM {table}")
            while batch := cursor.fetchmany(batch_size):
                store.load_history(table, [dict(zip(columns, row)) for row in batch])
                count += len(batch)
                logger.info('%s: loaded %d rows', table, count)
        counts[table] = count
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-size', type=int, default=100000)
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error('--batch-size must be positive')
    logger = create_logger()
    source = None
    client = None
    try:
        import psycopg2

        for name in ('CRYPTO_SYNC_DATABASE_URL', 'CRYPTO_SYNC_BQ_PROJECT',
                     'CRYPTO_SYNC_BQ_DATASET'):
            if not os.environ.get(name):
                raise ValueError(f'{name} is required')
        client = bigquery.Client(
            project=os.environ['CRYPTO_SYNC_BQ_PROJECT'],
            location=os.getenv('CRYPTO_SYNC_BQ_LOCATION') or None,
        )
        store = BigQueryStore(client, os.environ['CRYPTO_SYNC_BQ_DATASET'])
        store.ensure_tables()
        source = psycopg2.connect(os.environ['CRYPTO_SYNC_DATABASE_URL'])
        migrate(source, store, logger, args.batch_size)
        logger.info('Migration complete')
    except Exception as error:
        logger.error('Migration stopped (%s) at %s',
                     type(error).__name__, failure_location(error))
        return 1
    finally:
        if source is not None:
            source.close()
        if client is not None:
            client.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
