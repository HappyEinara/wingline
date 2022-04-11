"""Dynamodb helper."""

from wingline.types import PayloadIterable, PayloadIterator
from wingline.vendored.dynamodb import types as dynamodb_types


def dynamodb_deserialize(payloads: PayloadIterable) -> PayloadIterator:
    """Deserialize DynamoDb records."""

    deserialize = dynamodb_types.TypeDeserializer().deserialize

    for payload in payloads:

        # Strip the 'Item' key wrapper.
        if payload.get("Item"):
            payload = payload["Item"]

        payload = {k: deserialize(v) for k, v in payload.items()}
        yield payload
