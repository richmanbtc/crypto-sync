"""Application entry point. Importing this module has no side effects."""

import os
import signal

from google.cloud import bigquery

from .logger import create_logger, failure_location
from .panic_manager import PanicManager
from .synchronizer import Synchronizer
from .storage import BigQueryStore
from .utils import create_ccxt_client, validate_account_type


def load_settings(environ=None):
    env = os.environ if environ is None else environ
    required = ('CCXT_EXCHANGE', 'CRYPTO_SYNC_ACCOUNT', 'CRYPTO_SYNC_BQ_PROJECT', 'CRYPTO_SYNC_BQ_DATASET')
    for name in required:
        if not env.get(name):
            raise ValueError(f'{name} is required')
    account_type = env.get('CRYPTO_SYNC_ACCOUNT_TYPE') or None
    validate_account_type(env['CCXT_EXCHANGE'], account_type)
    interval = int(env.get('CRYPTO_SYNC_PANIC_INTERVAL', '300'))
    if interval <= 0:
        raise ValueError('CRYPTO_SYNC_PANIC_INTERVAL must be positive')
    return {
        'exchange': env['CCXT_EXCHANGE'],
        'account': env['CRYPTO_SYNC_ACCOUNT'],
        'project': env['CRYPTO_SYNC_BQ_PROJECT'],
        'dataset': env['CRYPTO_SYNC_BQ_DATASET'],
        'location': env.get('CRYPTO_SYNC_BQ_LOCATION') or None,
        'account_type': account_type,
        'api_key': env.get('CCXT_API_KEY'),
        'api_secret': env.get('CCXT_API_SECRET'),
        'api_password': env.get('CCXT_API_PASSWORD'),
        'log_level': env.get('CRYPTO_SYNC_LOG_LEVEL', 'INFO'),
        'panic_interval': interval,
    }


def start():
    settings = load_settings()
    logger = create_logger(settings['log_level'])
    panic_manager = PanicManager(logger=logger)
    interval = settings['panic_interval']
    panic_manager.register('bot', interval, interval)
    bq_client = bigquery.Client(project=settings['project'], location=settings['location'])
    store = BigQueryStore(bq_client, settings['dataset'])
    store.ensure_tables()
    client = create_ccxt_client(**{
        key: settings[key] for key in
        ('exchange', 'api_key', 'api_secret', 'api_password', 'account_type')
    })
    Synchronizer(
        client=client, logger=logger, store=store,
        account=settings['account'], account_type=settings['account_type'],
        health_check_ping=lambda: panic_manager.ping('bot'),
    ).run()


def main():
    try:
        start()
    except Exception as error:
        # Startup failures can include credential paths or private API responses.
        create_logger().error('Startup failed (%s) at %s',
                              type(error).__name__, failure_location(error))
        return 1
    return 0


if __name__ == '__main__':
    # PID 1 must handle SIGTERM explicitly; no shutdown work is needed.
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    raise SystemExit(main())
