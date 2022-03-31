"""Test the reader object."""

from pytest_cases import parametrize_with_cases

from wingline.files import filetype, reader


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_reader(path, content_hash, container, format, line_count):
    """The reader works end-to-end."""

    reader_object = reader.Reader(path)
    assert isinstance(reader_object.container, container)
    assert reader_object.format_type is format
    breakpoint()
    with reader_object as file_reader:
        first_line = next(file_reader)
        assert first_line
        assert isinstance(first_line, dict)


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_get_reader(path, content_hash, container, format, line_count):
    """Get a reader object for the file."""

    with filetype.get_reader(path) as file_reader:
        first_line = next(file_reader)
        assert first_line
        assert isinstance(first_line, dict)
