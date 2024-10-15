import click
import datetime
import logging
from typing import Optional


def configure_logging():
    # disable information from libraries logging to decrease output noise
    loggers = ["urllib3", "docker", "botocore"]
    for name in logging.root.manager.loggerDict:
        for logger in loggers:
            if name.startswith(logger):
                logging.getLogger(name).setLevel(logging.ERROR)


def global_logging():
    logging_format = "%(asctime)s,%(msecs)d %(levelname)s %(name)s: %(message)s"
    logging_date_format = "%H:%M:%S"
    logging.basicConfig(format=logging_format, datefmt=logging_date_format, level=logging.DEBUG)


class ColoredWrapper:
    SUCCESS = "\033[92m"
    STATUS = "\033[94m"
    WARNING = "\033[93m"
    ERROR = "\033[91m"
    BOLD = "\033[1m"
    END = "\033[0m"

    def __init__(self, prefix, logger, verbose=True, propagte=False):
        self.verbose = verbose
        self.propagte = propagte
        self.prefix = prefix
        self._logging = logger

    def debug(self, message):
        if self.verbose:
            self._print(message, ColoredWrapper.STATUS)
            if self.propagte:
                self._logging.debug(message)

    def info(self, message):
        self._print(message, ColoredWrapper.SUCCESS)
        if self.propagte:
            self._logging.info(message)

    def warning(self, message):
        self._print(message, ColoredWrapper.WARNING)
        if self.propagte:
            self._logging.warning(message)

    def error(self, message):
        self._print(message, ColoredWrapper.ERROR)
        if self.propagte:
            self._logging.error(message)

    def critical(self, message):
        self._print(message, ColoredWrapper.ERROR)
        if self.propagte:
            self._logging.critical(message)

    def _print(self, message, color):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")
        click.echo(
            f"{color}{ColoredWrapper.BOLD}[{timestamp}]{ColoredWrapper.END} "
            f"{ColoredWrapper.BOLD}{self.prefix}{ColoredWrapper.END} {message}",
            err=False,
        )


class LoggingHandlers:
    def __init__(self, verbose: bool = False, filename: Optional[str] = None):
        logging_format = "%(asctime)s,%(msecs)d %(levelname)s %(name)s: %(message)s"
        logging_date_format = "%H:%M:%S"
        formatter = logging.Formatter(logging_format, logging_date_format)
        self.handler: Optional[logging.FileHandler] = None

        # Remember verbosity for colored wrapper
        self.verbosity = verbose

        # Add file output if needed
        if filename:
            file_out = logging.FileHandler(filename=filename, mode="w")
            file_out.setFormatter(formatter)
            file_out.setLevel(logging.DEBUG if verbose else logging.INFO)
            self.handler = file_out


class LoggingBase:
    def __init__(self):
        if hasattr(self, "typename"):
            self.log_name = f"{self.typename()}"
        else:
            self.log_name = f"{self.__class__.__name__}"

        self._logging = logging.getLogger(self.log_name)
        self._logging.setLevel(logging.INFO)
        self.wrapper = ColoredWrapper(self.log_name, self._logging)

        console_handler = logging.StreamHandler()

        self._logging.addHandler(console_handler)

    @property
    def logging(self) -> ColoredWrapper:
        # This would always print log with color. And only if
        # filename in LoggingHandlers is set, it would log to file.
        return self.wrapper

    @property
    def logging_handlers(self) -> LoggingHandlers:
        return self._logging_handlers

    @logging_handlers.setter
    def logging_handlers(self, handlers: LoggingHandlers):
        self._logging_handlers = handlers

        self._logging.propagate = False
        self.wrapper = ColoredWrapper(
            self.log_name,
            self._logging,
            verbose=handlers.verbosity,
            propagte=handlers.handler is not None,
        )

        if self._logging_handlers.handler is not None:
            self._logging.addHandler(self._logging_handlers.handler)
