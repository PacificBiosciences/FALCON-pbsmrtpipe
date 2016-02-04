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
def run_pbalign(reads, asm, alignmentset):
    """
 BlasrService: Align reads to references using blasr.
 BlasrService: Call "blasr /pbi/dept/secondary/siv/testdata/SA3-DS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/polished-falcon-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_falcon2_run_asm-0/file.fasta -out /scratch/tmpTbV4Ec/wLCUdL.bam  -bam  -bestn 10 -minMatch 12  -nproc 16  -minSubreadLength 50 -minAlnLength 50  -minPctSimilarity 70 -minPctAccuracy 70 -hitPolicy randombest  -randomSeed 1  -minPctSimilarity 70.0 "
 FilterService: Filter alignments using samFilter.
 FilterService: Call "rm -f /scratch/tmpTbV4Ec/aM1Mor.bam && ln -s /scratch/tmpTbV4Ec/wLCUdL.bam /scratch/tmpTbV4Ec/aM1Mor.bam"
 BamPostService: Sort and build index for a bam file.
 BamPostService: Call "samtools sort -m 4G /scratch/tmpTbV4Ec/aM1Mor.bam /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/polished-falcon-lambda-007-tiny/job_output/tasks/pbalign.tasks.pbalign-0/aligned.subreads.alignmentset"
 BamPostService: Call "samtools index /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/polished-falcon-lambda-007-tiny/job_output/tasks/pbalign.tasks.pbalign-0/aligned.subreads.alignmentset.bam /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/polished-falcon-lambda-007-tiny/job_output/tasks/pbalign.tasks.pbalign-0/aligned.subreads.alignmentset.bam.bai"
 BamPostService: Call "pbindex /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/polished-falcon-lambda-007-tiny/job_output/tasks/pbalign.tasks.pbalign-0/aligned.subreads.alignmentset.bam"
] OutputService: Generating the output XML file
    """
    """
INFO:root:BlasrService: Align reads to references using blasr.
[INFO] 2016-02-04 14:50:25,434Z [root run 178] BlasrService: Align reads to references using blasr.
INFO:root:BlasrService: Call "blasr /pbi/dept/secondary/siv/testdata/SA3-DS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/asm.fasta -out /scratch/tmpwWCCmy/YrpIw_.bam  -bam  -bestn 10 -minMatch 12  -nproc 16  -minSubreadLength 50 -minAlnLength 50  -minPctSimilarity 70 -minPctAccuracy 70 -hitPolicy randombest  -randomSeed 1  -minPctSimilarity 70.0 "
[INFO] 2016-02-04 14:50:25,435Z [root Execute 63] BlasrService: Call "blasr /pbi/dept/secondary/siv/testdata/SA3-DS/lambda/2372215/0007_tiny/Analysis_Results/m150404_101626_42267_c100807920800000001823174110291514_s1_p0.all.subreadset.xml /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/asm.fasta -out /scratch/tmpwWCCmy/YrpIw_.bam  -bam  -bestn 10 -minMatch 12  -nproc 16  -minSubreadLength 50 -minAlnLength 50  -minPctSimilarity 70 -minPctAccuracy 70 -hitPolicy randombest  -randomSeed 1  -minPctSimilarity 70.0 "
INFO:root:FilterService: Filter alignments using samFilter.
[INFO] 2016-02-04 14:51:00,543Z [root run 150] FilterService: Filter alignments using samFilter.
INFO:root:FilterService: Call "rm -f /scratch/tmpwWCCmy/Ba3axn.bam && ln -s /scratch/tmpwWCCmy/YrpIw_.bam /scratch/tmpwWCCmy/Ba3axn.bam"
[INFO] 2016-02-04 14:51:00,544Z [root Execute 63] FilterService: Call "rm -f /scratch/tmpwWCCmy/Ba3axn.bam && ln -s /scratch/tmpwWCCmy/YrpIw_.bam /scratch/tmpwWCCmy/Ba3axn.bam"
INFO:root:BamPostService: Sort and build index for a bam file.
[INFO] 2016-02-04 14:51:00,692Z [root run 100] BamPostService: Sort and build index for a bam file.
INFO:root:BamPostService: Call "samtools sort -m 4G /scratch/tmpwWCCmy/Ba3axn.bam /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset"
[INFO] 2016-02-04 14:51:00,692Z [root Execute 63] BamPostService: Call "samtools sort -m 4G /scratch/tmpwWCCmy/Ba3axn.bam /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset"
INFO:root:BamPostService: Call "samtools index /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset.bam /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset.bam.bai"
[INFO] 2016-02-04 14:51:10,610Z [root Execute 63] BamPostService: Call "samtools index /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset.bam /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset.bam.bai"
INFO:root:BamPostService: Call "pbindex /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset.bam"
[INFO] 2016-02-04 14:51:12,162Z [root Execute 63] BamPostService: Call "pbindex /home/UNIXHOME/cdunn/repo/pb/smrtanalysis-client/smrtanalysis/siv/testkit-jobs/sa3_pipelines/hgap_lean-lambda-007-tiny/job_output/tasks/falcon_ns.tasks.task_hgap_prepare-0/aligned.subreads.alignmentset.bam"
INFO:root:OutputService: Generating the output XML file
[INFO] 2016-02-04 14:51:13,239Z [root _output 228] OutputService: Generating the output XML file
    """
    args = [
        'pbalign',
        '--verbose',
        #'--debug', # requires 'ipdb'
        #'--profile', # kinda interesting, but maybe slow?
        '--nproc 16',
        '--algorithmOptions "-minMatch 12 -bestn 10 -minPctSimilarity 70.0"',
        #'--concordant',
        '--hitPolicy randombest',
        '--minAccuracy 70.0',
        '--minLength 50',
        reads, asm,
        alignmentset,
    ]
    sys.system(' '.join(args))
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
def task_pbalign(self):
    reads = fn(self.dataset)
    asm = fn(self.referenceset)
    alignmentset = fn(self.alignmentset)
    run_pbalign(reads, asm, alignmentset)
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

    # pbalign (TODO: Look into chunking.)
    alignmentset_pfn = makePypeLocalFile('aligned.subreads.alignmentset.xml')
    """Also produces:
    aligned.subreads.alignmentset.bam
    aligned.subreads.alignmentset.bam.bai
    aligned.subreads.alignmentset.bam.pbi
    """
    parameters =  {
    }
    make_task = PypeTask(
            inputs = {"dataset": dataset_pfn,
                      "referenceset": referenceset_pfn,},
            outputs = {"alignmentset": alignmentset_pfn,},
            parameters = parameters,
            TaskType = PypeThreadTaskBase,
            URL = "task://localhost/pbalign")
    task = make_task(task_pbalign)
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
