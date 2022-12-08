import os
import dataset
from src.utils import (
    create_ccxt_client,
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

    if account is None or account == '':
        raise Exception('CRYPTO_SYNC_ACCOUNT empty')

    logger = create_logger(log_level)

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
    )

    synchronizer = Synchronizer(
        client=client,
        logger=logger,
        db=db,
        account=account,
        health_check_ping=health_check_ping,
    )
    synchronizer.run()


start()
