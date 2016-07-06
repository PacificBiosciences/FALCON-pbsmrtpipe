from pbfalcon import hgap_prepare as mod
from nose.tools import assert_equal
from StringIO import StringIO

def test_learn_job_type():
    script = """\
#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
qsub -S /bin/bash -sync y -V -q default -N job.7754426falcon_ns.tasks.task_hgap_prepare \
    -o "/home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap5_fake/synth5k/job_output/tasks/falcon_ns.tasks.t
    -e "/home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap5_fake/synth5k/job_output/tasks/falcon_ns.tasks.t
    -pe smp 16 \
    "/home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap5_fake/synth5k/job_output/tasks/falcon_ns.tasks.task
exit $?
"""
    job_type, sge_option = mod.learn_job_type(StringIO(script))
    assert_equal(job_type, 'sge')
    assert_equal(sge_option, '-q default')
