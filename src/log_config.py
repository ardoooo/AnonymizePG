import logging.config


def setup_logging(
    log_file_path="logs/logs.log", error_log_file_path="logs/error_logs.log"
):
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
                "level": "DEBUG",
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
