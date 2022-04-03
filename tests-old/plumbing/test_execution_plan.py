"""Test the execution plan optimiser."""
import logging

from pytest_cases import parametrize_with_cases

from wingline.plumbing import file, pipe, pipeline, sink
from wingline.types import PayloadIterable, PayloadIterator

logger = logging.getLogger(__name__)


def add_a(parent: PayloadIterable) -> PayloadIterator:
    for payload in parent:
        payload["_a"] = "a"
        yield payload


def add_b(parent: PayloadIterable) -> PayloadIterator:
    for payload in parent:
        payload["_b"] = "b"
        yield payload


def add_c(parent: PayloadIterable) -> PayloadIterator:
    for payload in parent:
        payload["_b"] = "b"
        yield payload


@parametrize_with_cases(
    "path,content_hash,container,format,item_count", cases="tests.cases.files"
)
def test_execution_plan(
    path, content_hash, container, format, item_count, testing_cache, tmp_path
):
    """The execution plan is accurate."""

    expected_classes = (sink.Sink, pipe.Pipe, pipe.Pipe, pipe.Pipe, file.File)
    test_pipe = (
        pipeline.Pipeline(file.File(path), cache_dir=tmp_path)
        .pipe(add_a)
        .pipe(add_b)
        .pipe(add_c)
    )
    plan = test_pipe.execution_plan
    logger.debug(plan.format())
    for step, expected_class in zip(plan.raw_steps, expected_classes, strict=True):
        assert isinstance(step, expected_class)
