import logging


class Logger:
    """
    Configure and use logs with logging for repository use.
    """

    @staticmethod
    def configureLog(level=logging.INFO, filename="run.log"):
        """
        Configure the logging settings.

        Args:
            level (int, optional): The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to INFO.
            filename (str, optional): The name of the log file. Defaults to 'your_log_file.log'.
        """
        logging.basicConfig(
            level=level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filename=filename,
        )

    @staticmethod
    def getLog(name):
        """
        Get a logger instance with the specified name.

        Args:
            name (str): The name of the logger.

        Returns:
            logging.Logger: A logger instance.
        """
        return logging.getLogger(name)
