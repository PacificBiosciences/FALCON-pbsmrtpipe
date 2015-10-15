from pbfalcon import ini2xml as m
from nose.tools import assert_equal
from StringIO import StringIO

cfg = """\
[General]
overlap_filtering_setting = --max_diff 100 --max_cov 50 --min_cov 1 --bestn 10 --n_core 24
other_setting = etc
"""

xml = """\
  <pipeline-template-preset>
    <task-options>
        <option id="falcon_ns.task_options.overlap_filtering_setting">
            <value>--max_diff 100 --max_cov 50 --min_cov 1 --bestn 10 --n_core
24</value>
        </option>
        <option id="falcon_ns.task_options.other_setting">
            <value>etc</value>
        </option>
    </task-options>
  </pipeline-template-preset>
"""

def test_xml():
    ifp = StringIO(cfg)
    ofp = StringIO()
    m.convert(ifp, ofp)
    got = ofp.getvalue()
    assert_equal(xml, got)
