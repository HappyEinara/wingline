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


@pytest.mark.parametrize("input_format", DATETIME_FORMATS)
@pytest.mark.parametrize("use_one_tuple", [True, False])
def test_datetimes(
    cases: List[Dict[str, Any]], input_format: str, use_one_tuple: bool
) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "datetime": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
            }
        )

    if use_one_tuple:
        test_pipeline = wingline.Pipeline(cases).process(
            helpers.datetime(("datetime",))
        )
    else:
        test_pipeline = wingline.Pipeline(cases).process(helpers.datetime("datetime"))
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["datetime"] == payload["expected_datetime"]


@pytest.mark.parametrize("input_format", DATE_FORMATS)
def test_dates(cases: List[Dict[str, Any]], input_format: str) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "date": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(helpers.date("date"))
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["date"] == payload["expected_date"]


@pytest.mark.parametrize("input_format", DATETIME_FORMATS)
def test_datetimes_with_explicit_input_format(
    cases: List[Dict[str, Any]], input_format: str
) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "datetime": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.datetime(("datetime", None, input_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["datetime"] == payload["expected_datetime"]


@pytest.mark.parametrize("input_format", DATE_FORMATS)
def test_dates_with_explicit_input_format(
    cases: List[Dict[str, Any]], input_format: str
) -> None:
    """Datetimes are successfully parsed."""

    for case in cases:
        case.update(
            {
                "date": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.date(("date", None, input_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["date"] == payload["expected_date"]


@pytest.mark.parametrize("input_format", DATETIME_FORMATS)
@pytest.mark.parametrize("output_format", DATETIME_FORMATS)
def test_datetimes_with_explicit_output_format(
    cases: List[Dict[str, Any]], input_format: str, output_format: str
) -> None:
    """Datetimes are successfully reformatted."""

    for case in cases:
        case.update(
            {
                "datetime": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
                "expected_output": case["expected_datetime"].strftime(output_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.datetime(("datetime", output_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["datetime"] == payload["expected_output"]
        assert payload["expected_datetime"] == dt.datetime.strptime(
            payload["expected_output"], output_format
        )


@pytest.mark.parametrize("input_format", DATE_FORMATS)
@pytest.mark.parametrize("output_format", DATE_FORMATS)
def test_dates_with_explicit_output_format(
    cases: List[Dict[str, Any]], input_format: str, output_format: str
) -> None:
    """Datetimes are successfully reformatted."""

    for case in cases:
        case.update(
            {
                "date": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
                "expected_output": case["expected_date"].strftime(output_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.date(("date", output_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["date"] == payload["expected_output"]
        assert (
            payload["expected_date"]
            == dt.datetime.strptime(payload["expected_output"], output_format).date()
        )


@pytest.mark.parametrize("input_format", DATETIME_FORMATS)
@pytest.mark.parametrize("output_format", DATETIME_FORMATS)
def test_datetimes_with_explicit_input_and_output_format(
    cases: List[Dict[str, Any]], input_format: str, output_format: str
) -> None:
    """Datetimes are successfully reformatted."""

    for case in cases:
        case.update(
            {
                "datetime": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
                "expected_output": case["expected_datetime"].strftime(output_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.datetime(("datetime", output_format, input_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["datetime"] == payload["expected_output"]
        assert payload["expected_datetime"] == dt.datetime.strptime(
            payload["expected_output"], output_format
        )


@pytest.mark.parametrize("input_format", DATE_FORMATS)
@pytest.mark.parametrize("output_format", DATE_FORMATS)
def test_dates_with_explicit_input_and_output_format(
    cases: List[Dict[str, Any]], input_format: str, output_format: str
) -> None:
    """Datetimes are successfully reformatted."""

    for case in cases:
        case.update(
            {
                "date": case["input"].strftime(input_format),
                "original": case["input"].strftime(input_format),
                "expected_output": case["expected_date"].strftime(output_format),
            }
        )

    test_pipeline = wingline.Pipeline(cases).process(
        helpers.date(("date", output_format, input_format))
    )
    result = list(test_pipeline)
    assert len(result) == len(cases)

    for payload in result:
        assert payload["date"] == payload["expected_output"]
        assert (
            payload["expected_date"]
            == dt.datetime.strptime(payload["expected_output"], output_format).date()
        )


@pytest.mark.parametrize(
    "bad_spec",
    [
        None,
        {"dict": "dict"},
        (
            "four",
            "elements",
            None,
            "wtf",
        ),
        ("stringnone", None),
        (
            "stringnonenone",
            None,
            None,
        ),
        (
            "stringstringnone",
            "stringstringnone",
            None,
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
