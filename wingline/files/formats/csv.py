"""The CSV adapter."""

import csv
import io
from typing import BinaryIO, Iterable

from wingline.files.formats import _base
from wingline.types import Payload


class Csv(_base.Format):
    """CSV format."""

    mime_type = "application/csv"
    suffixes = {".csv"}

    fieldnames = None
    restkey = "(rest)"
    restval = None
    dialect = "excel"
    extrasaction = "ignore"

    def read(self, handle: BinaryIO, **kwargs) -> Iterable[Payload]:
        """Dict iterator"""

        fieldnames = kwargs.get("fieldnames", self.fieldnames)
        restkey = kwargs.get("restkey", self.restkey)
        restval = kwargs.get("restval", self.restval)
        dialect = kwargs.get("dialect", self.dialect)

        text_handle = io.TextIOWrapper(handle, encoding="utf-8")
        reader = csv.DictReader(
            text_handle,
            fieldnames=fieldnames,
            restkey=restkey,
            restval=restval,
            dialect=dialect,
        )
        for line in reader:
            yield line

    def write(self, handle: BinaryIO, payload: Payload, **kwargs) -> None:
        """File writer."""

        fieldnames = kwargs.get("fieldnames", self.fieldnames)
        restval = kwargs.get("restval", self.restval)
        dialect = kwargs.get("dialect", self.dialect)
        extrasaction = kwargs.get("extrasaction", self.extrasaction)

        text_handle = io.TextIOWrapper(handle, encoding="utf-8")
        writer = csv.DictWriter(
            text_handle,
            fieldnames=fieldnames,
            restval=restval,
            dialect=dialect,
            extrasaction=extrasaction,
        )
        writer.writerow(payload)
