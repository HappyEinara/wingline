""""Intermediate cache."""

import pathlib

from wingline.files import containers, formats
from wingline.plumbing import file, pipe, writer

FILENAME_EXTENSION = ".wingline"
INTERMEDIATE_FORMAT = formats.Msgpack
INTERMEDIATE_CONTAINER = containers.Gzip


class IntermediateCache:
    def __init__(self, cache_dir: pathlib.Path):
        self.cache_dir = cache_dir

    def cache_path(self, hash: str) -> pathlib.Path:
        """Return the path for a given hash."""

        dir = self.cache_dir / hash[:2]
        path = dir / f"{hash}{FILENAME_EXTENSION}"
        return path

    def get_writer_pipe(self, pipe: pipe.Pipe):
        if not pipe.hash:
            raise ValueError("Can't get a cache writer for a pipe with no hash.")
        path = self.cache_path(pipe.hash)
        writer_pipe = writer.Writer(
            pipe, path, INTERMEDIATE_FORMAT, INTERMEDIATE_CONTAINER
        )
        return writer_pipe

    def get_reader_tap(self, path: pathlib.Path) -> file.IntermediateCacheFile:
        return file.IntermediateCacheFile(path)
