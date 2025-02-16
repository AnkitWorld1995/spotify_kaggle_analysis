"""
logging
~~~~~~~

This module contains a class that wraps the log4j object instantiated
by the active SparkContext, enabling Log4j logging for PySpark using.
"""

import logging


class LoggerWrapper:
    """
    Wrapper class for Python's logging module.

    :param name: Name of the logger (default: __name__)
    :param level: Logging level (default: logging.INFO)
    """

    def __init__(self, name=__name__, level=logging.INFO):
        # Configure logging
        logging.basicConfig(
            level=level,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(name)

    def error(self, message):
        """Log an error message."""
        self.logger.error(message)

    def warn(self, message):
        """Log a warning message."""
        self.logger.warning(message)

    def info(self, message):
        """Log an info message."""
        self.logger.info(message)
