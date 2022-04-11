"""Parse a DynamoDb export containing Wikidata entries on TV shows and cast members."""


import pathlib

from wingline import helpers
from wingline.helpers import keys
from wingline.pipeline import Pipeline
from wingline.types import PayloadIterable, PayloadIterator

INPUT_PATH = pathlib.Path(__file__).parents[2] / "data" / "dynamodb-tv-casts.jl.gz"


def filter_cast_members(payloads: PayloadIterable) -> PayloadIterator:
    """Filter records to return only records for actors."""

    for payload in payloads:
        if payload["PK"].startswith("#CASTMEMBER"):
            yield payload


def run_pipeline(input_path: pathlib.Path) -> None:
    """Run a simple but powerful pipeline."""

    pipeline = (
        Pipeline(input_path)
        .process(helpers.dynamodb_deserialize)
        .process(keys.filter("claims", "sitelinks", "PK"))
        .process(filter_cast_members)
        .pretty()
    )

    pipeline.run()
    pipeline.show_graph()


def main() -> None:
    """Run the main process."""

    input_path = INPUT_PATH

    if not input_path.exists():
        raise RuntimeError(f"Input path {input_path} doesn't exist!")

    print(f"🎉 Input path {input_path} found!")

    run_pipeline(input_path)


if __name__ == "__main__":
    main()
