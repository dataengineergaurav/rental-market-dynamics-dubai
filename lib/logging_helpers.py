"""Configure logging for the READ package."""

import logging

import coloredlogs


def get_logger(name: str):
    """Helper function to append 'READ' to logger name and return logger."""
    return logging.getLogger(f"READ.{name}")


def configure_root_logger(logfile: str, loglevel: str = "INFO"):
    """Configure the root READ logger.

    Args:
        logfile: Path to logfile or None.
        loglevel: Level of detail at which to log, by default INFO.
    """
    logger = logging.getLogger("READ")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=loglevel, logger=logger)

    logger.addHandler(logging.NullHandler())

    if logfile is not None:
        file_logger = logging.FileHandler(logfile)
        file_logger.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_logger)
