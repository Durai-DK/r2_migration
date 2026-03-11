import os, logging
from logging.config import dictConfig

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

success_log = logging.getLogger("success_logger")
error_log = logging.getLogger("error_logger")

LOGGING = {

    "version": 1,
    "disable_existing_loggers": False,

    "formatters": {
                "verbose": {"format": "[{asctime}] {levelname} {name} {message}", "style": "{",},
                "simple": {"format": "{levelname} {message}", "style": "{"},
    },

    "handlers": {
        # Success log handler
        "success_file": {
                        "level": "INFO",
                        "class": "logging.handlers.RotatingFileHandler",
                        "filename": os.path.join(LOG_DIR, "success.log"),
                        "maxBytes": 5 * 1024 * 1024,
                        "backupCount": 5,
                        "formatter": "verbose",
                        "encoding": "utf-8",
                    },

        # Error log handler
        "error_file": {
                        "level": "ERROR",
                        "class": "logging.handlers.RotatingFileHandler",
                        "filename": os.path.join(LOG_DIR, "error.log"),
                        "maxBytes": 5 * 1024 * 1024,
                        "backupCount": 5,
                        "formatter": "verbose",
                        "encoding": "utf-8",
                    },

        # Console handler
        "console": {"class": "logging.StreamHandler",
                    "formatter": "simple",
                    "level": "DEBUG"
        },
    },

    "loggers": {
        "success_logger": {
                        "handlers": ["success_file", "console"],
                        "level": "INFO",
                        "propagate": False,
                    },

        "error_logger": {
                        "handlers": ["error_file", "console"],
                        "level": "ERROR",
                        "propagate": False,
            },

        "uvicorn.error": {
                        "handlers": ["error_file", "console"],
                        "level": "ERROR",
                        "propagate": False,
        },

        "uvicorn.access": {
                        "handlers": ["success_file", "console"],
                        "level": "INFO",
                        "propagate": False,
        }
    },
}

dictConfig(LOGGING)
