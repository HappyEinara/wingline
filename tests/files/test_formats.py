"""Test format detection."""

from pytest_cases import parametrize_with_cases

from wingline.files import file


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_dynamo_gz(path, content_hash, container, format, line_count):
    """The format is JSON and the container is gz."""

    test_file = file.File(path)
    assert test_file.reader.format_type.mime_type == "application/json"
    assert test_file.reader.container.mime_type == "application/gzip"
