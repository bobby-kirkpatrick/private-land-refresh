import logging
import logging.handlers
from configs.settings import LOG_DIR

_LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(_LOG_FORMAT, _DATE_FORMAT)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    log_file = LOG_DIR / 'plr_pipeline.log'
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger
