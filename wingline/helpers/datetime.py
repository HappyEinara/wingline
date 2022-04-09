"""Date and time helpers"""

import datetime as dt
from typing import Optional, Tuple, Union, cast

from dateutil import parser

from wingline.types import PayloadIterable, PayloadIterator, PipeProcess

FieldSpec = Union[
    str,
    Tuple[str],
    Tuple[str, str],
    Tuple[str, Optional[str], str],
]


def parse_field_spec(field: FieldSpec) -> Tuple[str, Optional[str], Optional[str]]:
    """Validate and parse a field spec for date/datetime conversion."""

    if isinstance(field, str):
        return field, None, None
    if isinstance(field, tuple):
        if len(field) == 1:
            if isinstance(field[0], str):
                return field[0], None, None
        if len(field) == 2:
            field = cast(Tuple[str, str], field)
            if isinstance(field[0], str) and isinstance(field[1], str):
                return field[0], field[1], None
        if len(field) == 3:
            field = cast(Tuple[str, Optional[str], str], field)
            if (
                isinstance(field[0], str)
                and (isinstance(field[1], str) or field[1] is None)
                and isinstance(field[2], str)
            ):
                return field[0], field[1], field[2]
    raise ValueError("Invalid field spec.")


def datetime(*fields: FieldSpec, date_only: bool = False) -> PipeProcess:
    """Parse given fields into datetimes."""

    fields_spec: dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for field in fields:
        fieldname, output_format, input_format = parse_field_spec(field)
        fields_spec[fieldname] = output_format, input_format

    def _datetime(payloads: PayloadIterable) -> PayloadIterator:
        """Convert specified fields to datetimes."""

        for payload in payloads:
            for field, formats in fields_spec.items():
                output_format, input_format = formats
                if field in payload:
                    parsed: Union[dt.datetime, dt.date]
                    if input_format is None:
                        parsed = parser.parse(payload[field])
                    else:
                        parsed = dt.datetime.strptime(payload[field], input_format)
                    if date_only:
                        parsed = parsed.date()
                    if output_format is None:
                        payload[field] = parsed
                    else:
                        payload[field] = parsed.strftime(output_format)
            yield payload

    return _datetime


def date(*fields: FieldSpec) -> PipeProcess:
    """Parse given fields into dates."""

    return datetime(*fields, date_only=True)
