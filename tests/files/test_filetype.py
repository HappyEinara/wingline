"""Test filetype detection."""

from pytest_cases import parametrize_with_cases

from wingline.files import filetype


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_detect_container(path, content_hash, container, format, line_count):
    """File container correctly detected."""

    test_filetype = filetype.detect_container(path)
    assert test_filetype == container


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_get_container(path, content_hash, container, format, line_count):
    """File container correctly instantiated."""

    actual_container = filetype.get_container(path)
    assert isinstance(actual_container, container)
    assert actual_container.path == path


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_get_filetype(path, content_hash, container, format, line_count):
    """File format correctly detected."""
    actual_container = filetype.get_container(path)
    actual_filetype = filetype.detect_format(actual_container)

    assert actual_filetype == format
