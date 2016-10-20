from pbfalcon import hgap_prepare as mod
from nose.tools import assert_equal, assert_raises
from StringIO import StringIO

def test_learn_submit_template():
    start, stop = mod.learn_submit_template(script_json)
    assert_equal(start, 'qsub -S /bin/bash -sync y -V -q production -N ${JOB_ID} \\\n    -o \"${STDOUT_FILE}\" \\\n    -e \"${STDERR_FILE}\" \\\n    -pe smp ${NPROC} \\\n    \"${CMD}\"')
    assert_equal(stop, 'qdel ${JOB_ID}')

    assert_raises(RuntimeError, mod.learn_submit_template, script_local_json)


script_json = r"""
{
    "cluster": {
        "start": "qsub -S /bin/bash -sync y -V -q production -N ${JOB_ID} \\\n    -o \"${STDOUT_FILE}\" \\\n    -e \"${STDERR_FILE}\" \\\n    -pe smp ${NPROC} \\\n    \"${CMD}\"",
        "stop": "qdel ${JOB_ID}"
    }
}
"""

script_local_json = """\
{
    "cluster": null
}
"""
