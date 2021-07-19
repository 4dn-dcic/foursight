import pytest
import difflib
from chalicelib.checks.wrangler_checks import (
    get_tokens_to_string,
    string_label_similarity
)


@pytest.fixture
def in_out_cmp_score():
    return [
        ("one", "one", "one", 1),
        ("one two", "onetwo", "onetwo", 1),
        ("One Two", "onetwo", "one two", 1),
        ("Two one Four", "twoonefour", "Too One Four", 0.9),
        ("One-One-Ones", "oneoneones", "FixFixFix", 0),
        ("22-33 44", "223344", "TwoTwoThreeThreeFourFour", 0),
        ("A one&a two and a-3", "aone&atwoanda3", "atwo&aone anda3", 0.64),
    ]


def test_get_tokens_to_string(in_out_cmp_score):
    for tup in in_out_cmp_score:
        assert get_tokens_to_string(tup[0]) == tup[1]


def test_string_label_similarity(in_out_cmp_score):
    for tup in in_out_cmp_score:
        assert round(string_label_similarity(tup[0], tup[2]), 2) == tup[3]
