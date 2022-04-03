"""Wingline

A simple line-based data reader and translator
"""

# Importlib_metadata dependency can be removed when python 3.8 reaches EOL.
try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:  # pragma: no cover
    import importlib_metadata  # type: ignore

from wingline.json import json
from wingline.pipeline import Pipeline
from wingline.settings import settings

__version__ = importlib_metadata.version(__name__)

__all__ = [
    "Pipeline",
    "__version__",
    "json",
    "settings",
]
