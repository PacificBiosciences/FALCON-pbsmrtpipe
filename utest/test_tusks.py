from nose.tools import assert_equal
from pbfalcon import tusks
import pprint

def test_get_falcon_overrides():
    text = 'three=four; one = two;'
    overrides = tusks.get_falcon_overrides(text, 'foo')
    got = pprint.pformat(overrides)
    expected = "{'one': 'two', 'three': 'four'}"
    assert_equal(expected, got)

    # It should work even if the text is not semicolon delimited,
    # just in case.
    text = '\n[General]\nthree=four\none = two\n'
    overrides = tusks.get_falcon_overrides(text, 'foo')
    got = pprint.pformat(overrides)
    expected = "{'one': 'two', 'three': 'four'}"
    assert_equal(expected, got)
