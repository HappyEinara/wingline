"""Test cases."""


from wingline.files import containers, formats


def case_dynamodb_jsonl_gz(data_dir):
    """Gzipped, DynamoDb-serialized JsonLines."""

    # b2sum --length 64 --binary examples/data/dynamodb-tv-casts.jl.gz
    # c8e2e027a73751df *examples/data/dynamodb-tv-casts.jl.gz
    return (
        data_dir / "dynamodb-tv-casts.jl.gz",
        "c8e2e027a73751df",
        containers.Gzip,
        formats.JsonLines,
        85,
    )
