"""Test the CLI."""

from typer import testing

import wingline as package
from wingline import cli

runner = testing.CliRunner()


def test_cli_pingpong() -> None:
    """The pingpong command works."""

    result = runner.invoke(cli.app, ["ping"])
    assert result.exit_code == 0
    assert result.stdout == "pong\n"


def test_cli_version() -> None:
    """The version flag works."""

    result = runner.invoke(cli.app, ["--version"])
    assert result.exit_code == 0
    assert package.__version__ in result.stdout


def test_cli_debug() -> None:
    """Debug mode doesn't break anything."""

    result = runner.invoke(cli.app, ["--debug", "ping"])
    assert result.exit_code == 0
    assert result.stdout == "pong\n"


def test_cli_logfile() -> None:
    """Logging to file doesn't break anything."""

    result = runner.invoke(
        cli.app,
        [
            "--debug",
            "--log-dir=/tmp/wingline_tests",
            "ping",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == "pong\n"
