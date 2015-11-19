import pbfalcon.functional as func
import StringIO
from nose.tools import assert_equal, assert_raises


def test_calc_cutoff():
    pairs = {0: 1, 1:2, 2:3, 3:4}.items()
    total = 20
    def check(n, expected):
        got = func.calc_cutoff(n, pairs)
        assert_equal(expected, got)
    for n, expected in ((0, 3), (12, 3), (13, 2), (20, 1)):
        yield check, n, expected
    assert_raises(Exception, func.calc_cutoff, 21, pairs)

def test_total_length():
    pairs = {0: 1, 1:2, 2:3, 3:4}.items()
    total = func.total_length(pairs)
    assert_equal(20, total)

def test_fns_from_fofn():
    data = """
a-b
  
c.d
"""
    fofn = StringIO.StringIO(data)
    expected = ['a-b', 'c.d']
    got = list(func.fns_from_fofn(fofn))
    assert_equal(expected, got)
