import pytest

from numerai_reports import utils


@pytest.mark.parametrize('s, target', [
    ("fooBar", "foo_bar"),
    ("Foo", "foo"),
    ("FooBAR", "foo_bar")])
def test_to_snake_case(s, target):
    assert utils.to_snake_case(s) == target
