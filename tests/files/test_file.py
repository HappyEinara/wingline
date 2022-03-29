"""Test the file class."""

from pytest_cases import parametrize_with_cases

from wingline.files import file


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_file_hash(path, content_hash, container, format, line_count):
    """Test that file hashing is correct."""

    test_file = file.File(path)
    assert test_file.content_hash == content_hash
