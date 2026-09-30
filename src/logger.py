import logging
from pathlib import Path
from traceback import walk_tb


def create_logger(log_level='INFO'):
    level = getattr(logging, (log_level or 'INFO').upper(), None)
    if not isinstance(level, int):
        raise ValueError('Invalid CRYPTO_SYNC_LOG_LEVEL')
    logger = logging.getLogger('crypto_sync')
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)
    return logger


def failure_location(error):
    """Describe the stack without exception messages, source lines, or locals."""
    return ' -> '.join(
        f'{Path(frame.f_code.co_filename).name}:{line}:{frame.f_code.co_name}'
        for frame, line in walk_tb(error.__traceback__)
    ) or 'unknown'
