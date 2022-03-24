"""Basic placeholder tests."""

from wingline.pingpong import ping


def test_pingpong() -> None:
    """Test the pingpong placeholder function."""

    result = ping()
    assert result == "pong"
