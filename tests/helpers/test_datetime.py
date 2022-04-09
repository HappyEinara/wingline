"""Test datetime helpers."""

# pylint: disable = redefined-outer-name

import datetime as dt
from typing import Any, Callable, Dict, List

import pytest

import wingline
from wingline import helpers
from wingline.types import PipeProcess

# Cheatsheet: https://strftime.org/

DATE_FORMATS = (
    "%d/%b/%Y",
    "%d/%m/%Y",
    "%Y-%m-%d",
)

DATETIME_FORMATS = (
    "%d/%b/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)

DATETIMES = (
    dt.datetime(2022, 4, 13, 0, 0, 0),
    dt.datetime(2022, 4, 14, 12, 0, 0),
    dt.datetime(2022, 4, 15, 0, 0, 0),
    dt.datetime(2022, 4, 16, 12, 0, 0),
    dt.datetime(2022, 4, 17, 0, 0, 0),
    dt.datetime(2022, 4, 18, 12, 0, 0),
    dt.datetime(2022, 4, 19, 12, 0, 0),
)


@pytest.fixture
def datetime_cases() -> List[dt.datetime]:
    """Datetime cases."""

    return list(DATETIMES)


@pytest.fixture
def cases(datetime_cases: List[dt.datetime]) -> List[Dict[str, Any]]:
    """Payloads from the test cases."""

    return [
        {
            "input": dtc,
            "expected_date": dtc.date(),
            "expected_datetime": dtc,
        }
        for dtc in datetime_cases
    ]


@pytest.mark.parametrize("datetime_format", DATETIME_FORMATS)
def test_datetimes(cases: List[Dict[str, Any]], datetime_format: str) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "datetime": case["input"].strftime(datetime_format),
                "original": case["input"].strftime(datetime_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(helpers.datetime("datetime"))
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["datetime"] == payload["expected_datetime"]


@pytest.mark.parametrize("date_format", DATE_FORMATS)
def test_dates(cases: List[Dict[str, Any]], date_format: str) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "date": case["input"].strftime(date_format),
                "original": case["input"].strftime(date_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(helpers.date("date"))
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["date"] == payload["expected_date"]


@pytest.mark.parametrize("datetime_format", DATETIME_FORMATS)
def test_datetimes_with_explicit_format(
    cases: List[Dict[str, Any]], datetime_format: str
) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "datetime": case["input"].strftime(datetime_format),
                "original": case["input"].strftime(datetime_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.datetime(("datetime", datetime_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["datetime"] == payload["expected_datetime"]


@pytest.mark.parametrize("date_format", DATE_FORMATS)
def test_dates_with_explicit_format(
    cases: List[Dict[str, Any]], date_format: str
) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "date": case["input"].strftime(date_format),
                "original": case["input"].strftime(date_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.date(("date", date_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["date"] == payload["expected_date"]


@pytest.mark.parametrize(
    "bad_spec",
    [
        None,
        ("onestring",),
        {"dict": "dict"},
        (
            "three",
            "strings",
            "wtf",
        ),
    ],
)
@pytest.mark.parametrize("helper", [helpers.datetime, helpers.date])
def test_dates_bad_spec(
    cases: List[Dict[str, Any]],
    bad_spec: Any,
    helper: Callable[..., PipeProcess],
) -> None:
    """Test bad params to datetime and date fail."""

    with pytest.raises(ValueError):
        wingline.Pipeline(cases).process(helper(bad_spec))
