"""Test cases."""


def case_test_file(file):

    (
        path,
        _,
        container_type,
        format_type,
        line_count,
        expect_automatic_detection,
        size,
    ) = file
    return (
        path,
        container_type,
        format_type,
        line_count,
        expect_automatic_detection,
        size,
    )
