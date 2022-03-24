"""The CLI."""
# pylint: disable=unused-argument

import logging
import pathlib
import platform
from typing import Optional

import typer

import wingline as package
from wingline import log, pingpong, settings

logger = logging.getLogger(__name__)
app = typer.Typer()


def version_callback(output_version: bool = False) -> None:
    """Output the version."""

    version = package.__version__
    location = pathlib.Path(__file__).parent
    python_version = f"Python {platform.python_version()}"

    if output_version:
        typer.echo(f"Wingline v{version}")
        typer.echo(f"({python_version}, at {location})")
        raise typer.Exit()


@app.callback()
def callback(
    debug: bool = False,
    version: Optional[bool] = typer.Option(
        None, "--version", callback=version_callback, is_eager=True
    ),
    log_dir: Optional[pathlib.Path] = None,
) -> None:
    """The Wingline CLI."""
    if debug:
        settings.debug = True
        log.set_level(logging.DEBUG)
        logger.debug("In debug mode.")
    if log_dir:
        settings.log_dir = log_dir
        log_path = settings.log_dir / "wingline.log.txt"
        log.log_to_file(log_path)
        logger.debug("Logging to %s", log_path)


@app.command()
def ping() -> None:
    """Return the output of the ping command. Should be "pong"."""
    pong = pingpong.ping()
    logger.debug(pong)
    typer.echo(pong)
