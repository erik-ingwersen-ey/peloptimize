"""
Configure the logs for the ``pelopt`` package.
"""
import logging.config
import logging


LOGCONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {"format": "%(message)s"},
        "withlevel": {"format": "%(levelname)s - %(message)s"},
        "when": {"format": "[%(asctime)s | %(levelname)s] - %(message)s"},
        "whenAndWhere": {
            "format": "[%(asctime)s | %(levelname)s | %(processName)s | %("
            "pathname)s:%(module)s:%(lineno)s] - %(message)s"
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "whenAndWhere",
        }
    },
    "loggers": {"": {"level": "INFO", "handlers": ["console"]}},
}

logging.config.dictConfig(LOGCONFIG)
log = logging.getLogger()
log.setLevel(logging.INFO)
logger = log
