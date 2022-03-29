"""Test the writer object."""

from pytest_cases import parametrize_with_cases

from wingline.files import formats, reader, writer


@parametrize_with_cases(
    "path,content_hash,container,format,line_count", cases="tests.cases.files"
)
def test_writer(path, content_hash, container, format, line_count, tmp_path):
    """The writer works end-to-end."""

    stem = path.name.partition(".")[0]
    output_path = tmp_path / f"{stem}.wingline"
    reader_object = reader.Reader(path)
    writer_object = writer.Writer(output_path, formats.Msgpack)

    with reader_object as file_reader:
        with writer_object as file_write:
            for line in file_reader:
                file_write(line)

    with reader.Reader(path) as original_reader, reader.Reader(
        output_path
    ) as new_reader:
        for original, new in zip(original_reader, new_reader, strict=True):
            assert original == new
