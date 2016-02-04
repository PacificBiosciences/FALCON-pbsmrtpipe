from __future__ import absolute_import
from .. import sys

from pbcore.io import (SubreadSet, HdfSubreadSet, FastaReader, FastaWriter,
                       FastqReader, FastqWriter)
from pypeflow.controller import PypeThreadWorkflow
from pypeflow.data import PypeLocalFile, makePypeLocalFile, fn
from pypeflow.task import PypeTask, PypeThreadTaskBase
import gzip
import logging
import os
#import tempfile

log = logging.getLogger(__name__)

def run_bam_to_fastx(program_name, fastx_reader, fastx_writer,
                     input_file_name, output_file_name,
                     min_subread_length=0):
    """Copied from pbsmrtpipe/pb_tasks/pacbio.py,
    with minor changes.
    """
    # XXX this is really annoying; bam2fastx needs a --no-gzip feature
    #tmp_out_prefix = tempfile.NamedTemporaryFile().name
    tmp_out_prefix = 'out'
    args = [
        program_name,
        "-o", tmp_out_prefix,
        #'-c', '0', # does not help
        input_file_name,
    ]
    logging.info(" ".join(args))
    sys.system(' '.join(args))
    base_ext = program_name.replace("bam2", "")
    tmp_out = "{p}.{b}.gz".format(p=tmp_out_prefix, b=base_ext)
    assert os.path.isfile(tmp_out), tmp_out
    logging.info("raw output in {!r} to be re-written to {!r} (with min_subread_length={}).".format(
        tmp_out, output_file_name, min_subread_length))
    with gzip.open(tmp_out) as raw_in:
        with fastx_reader(raw_in) as fastx_in:
            with fastx_writer(output_file_name) as fastx_out:
                for rec in fastx_in:
                    if (min_subread_length < 1 or
                        min_subread_length < len(rec.sequence)):
                        fastx_out.writeRecord(rec)
def run_falcon(i_fasta_fn, o_fasta_fn):
    sys.system('cp -f {} {}'.format(
        i_fasta_fn, o_fasta_fn))
def task_bam2fasta(self):
    #print repr(self.parameters), repr(self.URL), repr(self.foo1)
    #sys.system('touch {}'.format(fn(self.fasta)))
    input_file_name = fn(self.dataset)
    output_file_name = fn(self.fasta)
    run_bam_to_fastx('bam2fasta', FastaReader, FastaWriter,
                     input_file_name, output_file_name,
                     min_subread_length=0)
def task_falcon(self):
    input_file_name = fn(self.orig_fasta)
    output_file_name = fn(self.asm_fasta)
    run_falcon(input_file_name, output_file_name)
def task_fasta2referenceset(self):
    """Copied from pbsmrtpipe/pb_tasks/pacbio.py:run_fasta_to_referenceset()
    """
    input_file_name = fn(self.fasta)
    output_file_name = fn(self.referenceset)
    cmd = 'dataset create --type ReferenceSet --generateIndices {} {}'.format(
            output_file_name, input_file_name)
    sys.system(cmd)
def task_foo(self):
    log.debug('WARNME1 {!r}'.format(__name__))
    #print repr(self.parameters), repr(self.URL), repr(self.foo1)
    sys.system('touch {}'.format(fn(self.foo2)))

def flow(config):
    #exitOnFailure=config['stop_all_jobs_on_failure'] # only matter for parallel jobs
    #wf.refreshTargets(exitOnFailure=exitOnFailure)
    #concurrent_jobs = config["pa_concurrent_jobs"]
    #PypeThreadWorkflow.setNumThreadAllowed(concurrent_jobs, concurrent_jobs)
    wf = PypeThreadWorkflow()

    dataset_pfn = makePypeLocalFile(config['pbsmrtpipe']['input_files'][0])
    orig_fasta_pfn = makePypeLocalFile('input.fasta')
    parameters =  {
            "fooparam": "barval",
    }
    make_task = PypeTask(
            inputs = {"dataset": dataset_pfn,},
            outputs =  {"fasta": orig_fasta_pfn,},
            parameters = parameters,
            TaskType = PypeThreadTaskBase,
            URL = "task://localhost/bam2fasta")
    task = make_task(task_bam2fasta)
    wf.addTask(task)
    wf.refreshTargets()

    # We could integrate the FALCON workflow here, but for now we will just execute it.
    asm_fasta_pfn = makePypeLocalFile('asm.fasta')
    parameters =  {
    }
    make_task = PypeTask(
            inputs =  {"orig_fasta": orig_fasta_pfn,},
            outputs =  {"asm_fasta": asm_fasta_pfn,},
            parameters = parameters,
            TaskType = PypeThreadTaskBase,
            URL = "task://localhost/falcon")
    #task = make_task(task_falcon)
    #wf.addTask(task)
    #wf.refreshTargets()
    run_falcon(fn(orig_fasta_pfn), fn(asm_fasta_pfn))

    # The reset of the workflow will operate on datasets, not fasta directly.
    referenceset_pfn = makePypeLocalFile('asm.referenceset.xml')
    parameters =  {
    }
    make_task = PypeTask(
            inputs =  {"fasta": asm_fasta_pfn,},
            outputs = {"referenceset": referenceset_pfn,},
            parameters = parameters,
            TaskType = PypeThreadTaskBase,
            URL = "task://localhost/fasta2referenceset")
    task = make_task(task_fasta2referenceset)
    wf.addTask(task)
    wf.refreshTargets()

    #return
    ##############

    if not os.path.exists('foo.bar1'):
        sys.system('touch foo.bar1')
    foo_fn1 = makePypeLocalFile('foo.bar1')
    foo_fn2 = makePypeLocalFile('foo.bar2')
    parameters =  {
            "fooparam": "barval",
    }
    make_task = PypeTask(
            inputs = {"foo1": foo_fn1,},
            outputs =  {"foo2": foo_fn2,},
            parameters = parameters,
            TaskType = PypeThreadTaskBase,
            URL = "task://localhost/foo")
    task = make_task(task_foo)
    wf.addTask(task)
    wf.refreshTargets()

    #raise Exception('hi')
