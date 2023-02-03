import os
import dataset
from src.utils import (
    create_ccxt_client,
    validate_account_type,
)
from .logger import create_logger
from .synchronizer import Synchronizer
from .panic_manager import PanicManager


def start():
    exchange = os.getenv('CCXT_EXCHANGE')
    api_key = os.getenv('CCXT_API_KEY')
    api_secret = os.getenv('CCXT_API_SECRET')
    api_password = os.getenv('CCXT_API_PASSWORD')
    log_level = os.getenv('CRYPTO_SYNC_LOG_LEVEL')
    account = os.getenv('CRYPTO_SYNC_ACCOUNT')
    account_type = os.getenv('CRYPTO_SYNC_ACCOUNT_TYPE', '')
    account_type = None if account_type == '' else account_type

    if account is None or account == '':
        raise Exception('CRYPTO_SYNC_ACCOUNT empty')

    validate_account_type(exchange, account_type)

    logger = create_logger(log_level)
    logger.info('exchange {}'.format(exchange))
    logger.info('account {}'.format(account))
    logger.info('account_type {}'.format(account_type))

    panic_manager = PanicManager(logger=logger)
    panic_manager.register('bot', 5 * 60, 5 * 60)
    def health_check_ping():
        panic_manager.ping('bot')

    database_url = os.getenv("CRYPTO_SYNC_DATABASE_URL")
    db = dataset.connect(database_url)

    client = create_ccxt_client(
        exchange=exchange,
        api_key=api_key,
        api_secret=api_secret,
        api_password=api_password,
        account_type=account_type,
    )

    synchronizer = Synchronizer(
        client=client,
        logger=logger,
        db=db,
        account=account,
        account_type=account_type,
        health_check_ping=health_check_ping,
    )
    synchronizer.run()


start()
