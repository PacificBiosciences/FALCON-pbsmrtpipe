from pbfalcon.gen_config import ini2option_text, option_text2ini
from nose.tools import assert_equal
from StringIO import StringIO

ini = """\
overlap_filtering_setting = --max_diff 100 --max_cov 50 --min_cov 1 --bestn 10 --n_core 24
y = foo
zother_setting = etc
"""

tab = '\t' +ini

ot = """\
overlap_filtering_setting = --max_diff 100 --max_cov 50 --min_cov 1 --bestn 10 --n_core 24;
y = foo; zother_setting = etc;
"""
ot_canonical = "overlap_filtering_setting = --max_diff 100 --max_cov 50 --min_cov 1 --bestn 10 --n_core 24;y = foo;zother_setting = etc;"

def test_ini2option_text():
    got = ini2option_text(ini)
    assert_equal(ot_canonical, got)

def test_option_text2ini():
    got = option_text2ini(ot)
    assert_equal(ini, got)
    got = option_text2ini(ot_canonical)
    assert_equal(ini, got)

def test_regress_sat709():
    got = ini2option_text(tab)
    assert_equal(ot_canonical, got)
