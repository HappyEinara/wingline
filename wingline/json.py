# Try to use ujson

try:
    import ujson as json
except ImportError:
    import json
