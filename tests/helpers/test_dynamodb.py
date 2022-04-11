"""Test the dynamodb deserialization helper."""

# pylint: disable=redefined-outer-name

from typing import Any, Dict, List

import pytest

import wingline
from wingline import helpers


@pytest.fixture
def ddb_input() -> List[Dict[str, Any]]:
    """Sample DynamoDb input."""

    # Example retrieved from
    # https://github.com/awsdocs/amazon-dynamodb-developer-guide/blob/6717f49e41f8206e4aa9bb2ce25de3b8f3e6d743/doc_source/DataExport.Output.md#dynamodb-json
    # Licenced under CC BY-SA 4.0 at
    # https://github.com/awsdocs/amazon-dynamodb-developer-guide/blob/master/LICENSE
    return [
        {
            "Item": {
                "Authors": {"SS": ["Author1", "Author2"]},
                "Dimensions": {"S": "8.5 x 11.0 x 1.5"},
                "ISBN": {"S": "333-3333333333"},
                "Id": {"N": "103"},
                "InPublication": {"BOOL": False},
                "PageCount": {"N": "600"},
                "Price": {"N": "2000"},
                "ProductCategory": {"S": "Book"},
                "Title": {"S": "Book 103 Title"},
            }
        }
    ]


@pytest.fixture
def ddb_expected() -> List[Dict[str, Any]]:
    """Expected deserialization."""
    return [
        {
            "Authors": {"Author1", "Author2"},
            "Dimensions": "8.5 x 11.0 x 1.5",
            "ISBN": "333-3333333333",
            "Id": 103,
            "InPublication": False,
            "PageCount": 600,
            "Price": 2000,
            "ProductCategory": "Book",
            "Title": "Book 103 Title",
        }
    ]


def test_dynamodb_deserializer(
    ddb_input: List[Dict[str, Any]], ddb_expected: List[Dict[str, Any]]
) -> None:
    """Test the dynamodb deserialization helper."""

    ddb_pipeline = wingline.Pipeline(ddb_input).all(helpers.dynamodb_deserialize)
    result = list(ddb_pipeline)
    assert result == ddb_expected
