"""Date and time helpers"""

import datetime as dt
from typing import Optional, Union

from dateutil import parser

from wingline.types import PayloadIterable, PayloadIterator, PipeProcess


def datetime(
    *fields: Union[str, tuple[str, str]], date_only: bool = False
) -> PipeProcess:
    """Parse given fields into datetimes."""

    fields_spec: dict[str, Optional[str]] = {}
    for field in fields:
        if isinstance(field, str):
            fields_spec[field] = None
        elif isinstance(field, tuple) and len(field) == 2:
            fields_spec[field[0]] = field[1]
        else:
            raise ValueError(
                "The datetime and date helpers expect fieldname arguments to be "
                "either a string or a (name, strptime-format) tuple."
            )

    def _date(payloads: PayloadIterable) -> PayloadIterator:
        """Convert specified fields to datetimes."""

        for payload in payloads:
            for field, datetime_format in fields_spec.items():
                if field in payload:
                    parsed: Union[dt.datetime, dt.date]
                    if datetime_format is None:
                        parsed = parser.parse(payload[field])
                    else:
                        parsed = dt.datetime.strptime(payload[field], datetime_format)
                    if date_only:
                        parsed = parsed.date()
                    payload[field] = parsed
            yield payload

    return _date


def date(*fields: Union[str, tuple[str, str]]) -> PipeProcess:
    """Parse given fields into dates."""

    return datetime(*fields, date_only=True)
