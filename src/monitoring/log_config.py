import logging.config


def setup_logging(
    log_file_path: str,
    error_log_file_path: str,
    disable_logging=False,
):
    if disable_logging:
        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": True,
                "handlers": {
                    "null": {
                        "level": "NOTSET",
                        "class": "logging.NullHandler",
                    },
                },
                "loggers": {
                    "": {
                        "handlers": ["null"],
                        "level": "NOTSET",
                        "propagate": False,
                    }
                },
            }
        )
        return

    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s.%(msecs)03d - [%(levelname)s] - %(name)s : %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "detailed",
            },
            "file_all": {
                "level": "DEBUG",
                "class": "logging.FileHandler",
                "filename": log_file_path,
                "formatter": "detailed",
            },
            "file_error": {
                "level": "ERROR",
                "class": "logging.FileHandler",
                "filename": error_log_file_path,
                "formatter": "detailed",
            },
        },
        "loggers": {
            "": {  # root logger
                "handlers": ["console", "file_all", "file_error"],
                "level": "DEBUG",
                "propagate": True,
            }
        },
    }

    logging.config.dictConfig(LOGGING_CONFIG)
