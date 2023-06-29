import loguru
import sys

def configure_logger(name) -> None:

    loguru.logger.configure(
        handlers=[
            {
                "sink": sys.stdout,
                "colorize": True,
                "format": f"{name} " + "| {level} | {message}",
            }
        ]
    )