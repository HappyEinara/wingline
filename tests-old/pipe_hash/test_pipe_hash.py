"""Test the hashing of pipe operations."""


from pytest_cases import parametrize_with_cases

from wingline import hasher


@parametrize_with_cases("func,func_hash", cases="tests.cases.operations")
def test_callable_hashing(func, func_hash):

    test_hash = hasher.hash_callable(func)
    assert test_hash == func_hash
