"""Parse a DynamoDb export containing Wikidata entries on TV shows and cast members."""


import pathlib
from typing import Optional

from wingline import helpers
from wingline.helpers import keys
from wingline.pipeline import Pipeline
from wingline.types import Payload, PayloadIterable, PayloadIterator

INPUT_PATH = pathlib.Path(__file__).parents[2] / "data" / "dynamodb-tv-casts.jl.gz"
CACHE_DIR = "~/.cache/wingline-tutorial"


def filter_cast_members(payloads: PayloadIterable) -> PayloadIterator:
    """Filter records to return only records for actors."""

    for payload in payloads:
        if payload["PK"].startswith("#CASTMEMBER"):
            yield payload


def get_imdb_id(payload: Payload) -> Optional[Payload]:
    """Extract the imDb ID for each castmember."""

    payload["imdb_id"] = (
        payload.get("claims", {})
        .get("P345", [{}])[0]
        .get("mainsnak", {})
        .get("datavalue", {})
        .get("value")
    )
    del payload["claims"]
    return payload


def get_wikipedia_page(payload: Payload) -> Optional[Payload]:
    """Get the title and URL of the actor's Wikipedia page."""

    wiki_data = payload.get("sitelinks", {}).get("enwiki", {})
    payload["wikipedia_title"] = wiki_data.get("title")
    payload["wikipedia_url"] = wiki_data.get("url")
    del payload["sitelinks"]
    return payload


def run_pipeline(input_path: pathlib.Path) -> None:
    """Run a simple but powerful pipeline."""

    pipeline = (
        Pipeline(input_path, cache_dir=CACHE_DIR)
        .process(helpers.dynamodb_deserialize)
        .process(keys.filter("claims", "sitelinks", "PK"))
        .process(filter_cast_members)
        .process(keys.remove("PK"))
        .each(get_imdb_id)
        .each(get_wikipedia_page)
        .process(keys.pascal)
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
