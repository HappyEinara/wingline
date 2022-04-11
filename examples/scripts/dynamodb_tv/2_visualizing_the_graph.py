"""Parse a DynamoDb export containing Wikidata entries on TV shows and cast members."""


import pathlib

from wingline.pipeline import Pipeline

INPUT_PATH = pathlib.Path(__file__).parents[2] / "data" / "dynamodb-tv-casts.jl.gz"


def run_pipeline(input_path: pathlib.Path) -> None:
    """Run a simple but powerful pipeline."""

    pipeline = Pipeline(input_path).pretty()

    pipeline.show_graph()
    # pipeline.run()


def main() -> None:
    """Run the main process."""

    input_path = INPUT_PATH

    if not input_path.exists():
        raise RuntimeError(f"Input path {input_path} doesn't exist!")

    print(f"🎉 Input path {input_path} found!")

    run_pipeline(input_path)


if __name__ == "__main__":
    main()
