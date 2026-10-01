"""Application entry point. Importing this module has no side effects."""

import os
import random
import signal
import time

from .logger import create_logger, failure_location
from .panic_manager import PanicManager


class ConfigurationError(ValueError):
    """A configuration error containing only fixed names and safe messages."""


def load_settings(environ=None):
    env = os.environ if environ is None else environ
    required = ('CCXT_EXCHANGE', 'CRYPTO_SYNC_ACCOUNT', 'CRYPTO_SYNC_BQ_PROJECT', 'CRYPTO_SYNC_BQ_DATASET')
    missing = [name for name in required if not env.get(name)]
    if missing:
        raise ConfigurationError('Required settings missing: ' + ', '.join(missing))
    account_type = env.get('CRYPTO_SYNC_ACCOUNT_TYPE') or None
    allowed = (None, 'btc', 'eth', 'unified') if env['CCXT_EXCHANGE'] == 'bybit' else (None,)
    if account_type not in allowed:
        raise ConfigurationError('Invalid CRYPTO_SYNC_ACCOUNT_TYPE')
    try:
        interval = int(env.get('CRYPTO_SYNC_PANIC_INTERVAL', '300'))
    except ValueError:
        raise ConfigurationError('CRYPTO_SYNC_PANIC_INTERVAL must be a positive integer') from None
    if interval <= 0:
        raise ConfigurationError('CRYPTO_SYNC_PANIC_INTERVAL must be positive')
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
    try:
        logger = create_logger(settings['log_level'])
    except ValueError:
        raise ConfigurationError('Invalid CRYPTO_SYNC_LOG_LEVEL') from None

    # Reject bad settings before loading the exchange and Google SDKs.
    from google.cloud import bigquery
    from .storage import BigQueryStore
    from .synchronizer import Synchronizer
    from .utils import create_ccxt_client

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
        delay = random.uniform(5, 10)
        logger = create_logger()
        # Startup failures can include credential paths or private API responses.
        if isinstance(error, ConfigurationError):
            logger.error('Startup failed: %s; exiting in %.1fs', error, delay)
        else:
            logger.error('Startup failed (%s) at %s; exiting in %.1fs',
                         type(error).__name__, failure_location(error), delay)
        time.sleep(delay)
        return 1
    return 0


if __name__ == '__main__':
    # PID 1 must handle SIGTERM explicitly; no shutdown work is needed.
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    raise SystemExit(main())
