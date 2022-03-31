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


def case_csv(data_dir):
    """Plain CSV."""

    # b2sum --length 64 --binary examples/data/2016Census_G01_NSW_LGA.csv
    # cbd95cd394278dc5 *examples/data/2016Census_G01_NSW_LGA.csv

    return (
        data_dir / "2016Census_G01_NSW_LGA.csv",
        "cbd95cd394278dc5",
        containers.Container,
        formats.Csv,
        85,
    )


# def case_csv_zip(data_dir):
#     """Zipped CSV."""

#     # b2sum --length 64 --binary examples/data/2016_GCP_LGA_for_NSW_short-header.zip
#     # d89d6200b8975e2e *examples/data/2016_GCP_LGA_for_NSW_short-header.zip
#     return (
#         data_dir / "2016_GCP_LGA_for_NSW_short-header.zip",
#         "d89d6200b8975e2e",
#         containers.Zip,
#         formats.Csv,
#         85,
#     )
