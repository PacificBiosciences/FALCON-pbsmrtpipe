import pbfalcon.functional as func
from nose.tools import assert_equal, assert_raises


def test_calc_cutoff():
    pairs = {0: 1, 1:2, 2:3, 3:4}.items()
    total = 20
    gt0 = func.calc_cutoff(0, pairs)
    assert_equal(3, gt0)
    gt12 = func.calc_cutoff(12, pairs)
    assert_equal(3, gt12)
    gt13 = func.calc_cutoff(13, pairs)
    assert_equal(2, gt13)
    gt20 = func.calc_cutoff(20, pairs)
    assert_equal(1, gt20)
    assert_raises(Exception, func.calc_cutoff, 21, pairs)

def test_total_length():
    pairs = {0: 1, 1:2, 2:3, 3:4}.items()
    total = func.total_length(pairs)
    assert_equal(20, total)
