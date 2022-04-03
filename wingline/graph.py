"""Class to represent pipelines as a graph."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wingline import pipeline


class PipelineGraph:
    def __init__(self, pipeline: pipeline.Pipeline):
        self.pipeline = pipeline

    @property
    def _graph(self) -> dict[str, list[str]]:
        """Return the graph as a dict."""

    def dict(self) -> dict[str, list[str]]:
        """Return the graph as a dict."""

        return self._graph
