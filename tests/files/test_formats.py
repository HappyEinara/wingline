"""Test format detection."""

from pytest_cases import parametrize_with_cases

from wingline.files import file


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_format_and_container(path, content_hash, container, format, line_count):
    """The format is JSON and the container is gz."""

    test_file = file.File(path)
    assert test_file.reader.format_type == format
    assert type(test_file.reader.container) == container
    assert test_file.content_hash == content_hash
