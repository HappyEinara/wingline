"""Parse a DynamoDb export containing Wikidata entries on TV shows and cast members."""


import pathlib

INPUT_PATH = pathlib.Path(__file__).parents[2] / "data" / "dynamodb-tv-casts.jl.gz"


def main() -> None:
    """Run the main process."""

    input_path = INPUT_PATH

    if not input_path.exists():
        raise RuntimeError(f"Input path {input_path} doesn't exist!")

    print(f"🎉 Input path {input_path} found!")


if __name__ == "__main__":
    main()
