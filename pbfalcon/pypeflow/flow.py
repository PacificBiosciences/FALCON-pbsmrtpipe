from __future__ import absolute_import
from .. import sys
from .. import report_preassembly

from pbcore.io import (SubreadSet, HdfSubreadSet, FastaReader, FastaWriter,
                       AlignmentSet, ReferenceSet, ContigSet,
                       FastqReader, FastqWriter)
from pbcoretools.tasks.converters import run_bam_to_fasta
from pbcoretools.chunking.gather import gather_alignmentset
from pypeflow.pwatcher_bridge import PypeProcWatcherWorkflow, MyFakePypeThreadTaskBase
from pypeflow.controller import PypeThreadWorkflow
from pypeflow.data import PypeLocalFile, makePypeLocalFile, fn
from pypeflow.task import PypeTask, PypeThreadTaskBase
import pysam

import contextlib
import gzip
import json
import logging
import os
import pprint
import re
import cStringIO

log = logging.getLogger(__name__)

#PypeWorkflow = PypeThreadWorkflow
#PypeTaskBase = PypeThreadTaskBase
PypeWorkflow = PypeProcWatcherWorkflow
PypeTaskBase = MyFakePypeThreadTaskBase

def say(x):
    log.warning(x)
    print 'IN FLOW!'
def HOLD_run_bam_to_fastx(program_name, fastx_reader, fastx_writer,
                     input_file_name, output_file_name,
                     min_subread_length=0):
    """Copied from pbsmrtpipe/pb_tasks/pacbio.py,
    with minor changes.
    That was later moved to pbcoretools/tasks/converters.py,
    and that was recently changed enough that something here is completely broken.
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
def updated_cfg(options_dict):
    opts = dict()
    for key, val in options_dict.iteritems():
        # Drop comments (keys w/ leading ~).
        if key.startswith('~'):
            continue
        # Strip leading and trailing ws, just in case.
        opts[key] = val
    opts['job_type'] = 'local' # OVERRIDE FOR NOW
    def add(key, val):
        if not key in opts:
            opts[key] = val
    add('input_fofn', 'NA') # actually, we do not need this anymore
    add('target', 'assembly')
    add('sge_option_da', 'NA')
    add('sge_option_la', 'NA')
    add('sge_option_pda', 'NA')
    add('sge_option_pla', 'NA')
    add('sge_option_fc', 'NA')
    add('sge_option_cns', 'NA')
    return opts
def dict2ini(ofs, options_dict):
    ofs.write('[General]\n')
    for key, val in sorted(options_dict.items()):
        ofs.write('{} = {}\n'.format(key, val))
def dict2json(ofs, options_dict):
    content = json.dumps(options_dict, sort_keys=True, indent=4, separators=(',', ': '))
    ofs.write(content + '\n')

@contextlib.contextmanager
def ContentUpdater(fn):
    """Write new content only if differs from old.
    """
    if os.path.exists(fn):
        with open(fn) as f:
            old_content = f.read()
    else:
        old_content = None
    new_writer = cStringIO.StringIO()
    yield new_writer
    new_content = new_writer.getvalue()
    if new_content != old_content:
        with open(fn, 'w') as f:
            f.write(new_content)
def run_prepare_falcon(falcon_parameters, i_fasta_fn, fc_cfg_fn, fc_json_config_fn, input_fofn_fn):
    with ContentUpdater(input_fofn_fn) as f:
        f.write('{}\n'.format(i_fasta_fn))
    config_falcon = updated_cfg(dict(falcon_parameters))
    config_falcon['input_fofn'] = input_fofn_fn
    with ContentUpdater(fc_cfg_fn) as f:
        dict2ini(f, config_falcon)
    with ContentUpdater(fc_json_config_fn) as f:
        dict2json(f, config_falcon)
def run_falcon(fc_cfg_fn, o_fasta_fn, preads_fofn_fn):
    sys.unlink(o_fasta_fn, preads_fofn_fn)
    #TODO: Let falcon use logging.json?
    cmd = 'fc_run1 {}'.format(
        fc_cfg_fn)
    sys.system(cmd)
    sys.symlink('2-asm-falcon/p_ctg.fa', o_fasta_fn)
    sys.symlink('1-preads_ovl/input_preads.fofn', preads_fofn_fn)
def run_report_pre_assembly(i_json_config_fn, i_raw_reads_fofn_fn, i_preads_fofn_fn, o_json_fn):
    kwds = {
        'i_json_config_fn': i_json_config_fn,
        'i_raw_reads_fofn_fn': i_raw_reads_fofn_fn,
        'i_preads_fofn_fn': i_preads_fofn_fn,
        'o_json_fn': o_json_fn,
    }
    report_preassembly.for_task(**kwds)
def run_pbalign_scatter(subreads, ds_reference, cjson_out):
    args = ['python -m pbcoretools.tasks.scatter_subread_reference',
        '-v',
        '--max_nchunks=5',
        subreads,
        ds_reference,
        cjson_out,
    ]
    sys.system(' '.join(args))
def run_pbalign_gather(alignmentsets, ds_out):
    #'python -m pbcoretools.tasks.gather_alignments cjson_in ds_out'
    #args = ['python -c "from pbcoretools.chunking.gather import gather_alignmentset; gather_alignmentset(input_files, output_file)"']
    input_files = alignmentsets
    output_file = ds_out #alignmentset
    gather_alignmentset(input_files, output_file)
def run_gc_scatter(alignmentset_fn, referenceset_fn, chunks_fofn_fn):
    #'python -m pbcoretools.tasks.scatter_alignments_reference alignment_ds ds_reference json_out'
    dir_name = os.getcwd()
    dset = AlignmentSet(alignmentset_fn, strict=True)
    maxChunks = 2
    dset_chunks = dset.split(contigs=True, maxChunks=maxChunks, breakContigs=True)

    # referenceset is used only for sanity checking.
    ReferenceSet(referenceset_fn, strict=True)

    chunk_fns = []
    for i, dset in enumerate(dset_chunks):
        chunk_name = 'chunk_alignmentset_{}.alignmentset.xml'.format(i)
        chunk_fn = os.path.join(dir_name, chunk_name)
        dset.write(chunk_fn)
        chunk_fns.append(chunk_fn)
    with open(chunks_fofn_fn, 'w') as ofs:
        for fn in chunk_fns:
            ofs.write('{}\n'.format(fn))
    log.info('Wrote {} chunks into "{}"'.format(len(dset_chunks), chunks_fofn_fn))
def __gather_contigset(input_files, output_file, new_resource_file):
    """Copied from pbcoretools.chunking.gather:__gather_contigset()
    """
    skip_empty = True
    if skip_empty:
        _input_files = []
        for file_name in input_files:
            cs = ContigSet(file_name)
            if len(cs.toExternalFiles()) > 0:
                _input_files.append(file_name)
        input_files = _input_files
    tbr = ContigSet(*input_files)
    tbr.consolidate(new_resource_file)
    tbr.newUuid()
    tbr.write(output_file)
    return output_file
def run_gc_gather(dset_fns, ds_out_fn):
    """Gather contigsets/fasta into 1 contigset/fasta.
    For some reason, we do not bother with fastq. That
    is handled elsewhere, but using this contigset implicitly.
    """
    # python -m pbcoretools.tasks.gather_contigs --rtc
    log.info('Gathering {!r} from chunks {!r}'.format(ds_out_fn, dset_fns))
    assert ds_out_fn.endswith('xml')
    new_resource_fn = os.path.splitext(ds_out_fn)[0] + '.fasta'
    __gather_contigset(dset_fns, ds_out_fn, new_resource_fn)
def run_gc_gather_fastq(fastq_fns, fastq_fn):
    """Also writes fastq.contigset.xml
    """
    from pbcoretools.chunking.gather import gather_fastq_contigset
    log.info('gc_gather_fastq({!r}, {!r})'.format(fastq_fns, fastq_fn))
    assert fastq_fns, 'Empty list. gather_fastq_contigset() would produce nothing.'
    gather_fastq_contigset(fastq_fns, fastq_fn)
def run_pbalign(reads, asm, alignmentset, options, algorithmOptions):
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
    args = [
        'pbalign',
        '--verbose',
        #'--debug', # requires 'ipdb'
        #'--profile', # kinda interesting, but maybe slow?
        '--nproc 16',
        #'--algorithmOptions "-minMatch 12 -bestn 10 -minPctSimilarity 70.0"',
        #'--concordant',
        #'--hitPolicy randombest',
        #'--minAccuracy 70.0',
        #'--minLength 50',
        options,
        '--algorithmOptions "%s"' %algorithmOptions,
        reads, asm,
        alignmentset,
    ]
    sys.system(' '.join(args))
def run_gc(alignmentset, referenceset, polished_fastq, variants_gff, consensus_contigset, options):
    """GenomicConsensus
    Note: GenomicConsensus outputs .fasta instead of .contigset.xml, so
    there is weird code in its resolved_tool_contract_runner() which substitutes
    a filename and later converts .fasta to a contigset. That does not exist
    for the non-pbcommand entry-point, so we copy that code here.
    TODO: Capture log output? Or figure out how to log to a file.
    """
    dataset_path = consensus_contigset
    fasta_path = re.sub(".contigset.xml", ".fasta", dataset_path)
    args = [
        'variantCaller',
        '--log-level DEBUG',
        #'--log-file foo.log',
        #'--verbose',
        #'--debug', # requires 'ipdb'
        #'-j NWORKERS',
        #'--algorithm quiver',
        #'--diploid', # binary
        #'--minConfidence 40',
        #'--minCoverage 5',
        ' --alignmentSetRefWindows',
        options,
        '--referenceFilename', referenceset,
        '-o', polished_fastq,
        '-o', variants_gff,
        '-o', fasta_path,
        alignmentset,
    ]
    sys.system(' '.join(args))

    # Convert to contigset.xml
    pysam.faidx(fasta_path)
    ds = ContigSet(fasta_path, strict=True)
    ds.write(dataset_path)
def run_summarize_coverage(gathered_alignmentset_fn, referenceset_fn, alignment_summary_gff_fn, options):
    """
    from pbreports.report.summarize_coverage.summarize_coverage import summarize_coverage
    summarize_coverage(args.aln_set, args.aln_summ_gff, args.ref_set,
                       args.num_regions, args.region_size,
                       args.force_num_regions)
    """
    args = ['python -m pbreports.report.summarize_coverage.summarize_coverage',
        options,
        gathered_alignmentset_fn,
        referenceset_fn,
        alignment_summary_gff_fn,
    ]
    sys.system(' '.join(args))
def run_polished_assembly_report(alignment_summary_gff_fn, polished_fastq_fn, report_fn):
    args = ['python -m pbreports.report.polished_assembly',
        alignment_summary_gff_fn,
        polished_fastq_fn,
        report_fn,
    ]
    sys.system(' '.join(args))
def run_filterbam(ifn, ofn, config):
    """
    pbcoretools.tasks.filterdataset
    python -m pbcoretools.tasks.filters  run-rtc
        "options": {
            "pbcoretools.task_options.other_filters": "rq >= 0.7",
            "pbcoretools.task_options.read_length": 0
        },
    """
    other_filters = config.get('other_filters', 'rq >= 0.7')
    read_length = config.get('read_length', 0)
    from pbcoretools.tasks.filters import run_filter_dataset
    log.warning('RUNNING filterbam: %r %r\n%r\n%r' %(read_length, other_filters, ifn, ofn))
    rc = run_filter_dataset(str(ifn), (ofn), read_length=str(read_length), other_filters=(other_filters))
    log.warning('FINISHED filterbam(rc=%r): %r %r' %(rc, read_length, other_filters))
def task_filterbam(self):
    #print repr(self.parameters), repr(self.URL), repr(self.foo1)
    #sys.system('touch {}'.format(fn(self.fasta)))
    input_file_name = fn(self.i_dataset)
    output_file_name = fn(self.o_dataset)
    config = self.parameters.get('pbcoretools.tasks.filterdataset', None) # TODO: Drop this.
    if not config:
        config = self.parameters['pbcoretools']
    run_filterbam(input_file_name, output_file_name, config)
def task_bam2fasta(self):
    """
    Same as bam2fasta_nofilter in pbcoretools.
    """
    #print repr(self.parameters), repr(self.URL), repr(self.foo1)
    #sys.system('touch {}'.format(fn(self.fasta)))
    input_file_name = fn(self.dataset)
    output_file_name = fn(self.fasta)
    #run_bam_to_fastx('bam2fasta', FastaReader, FastaWriter,
    #                 input_file_name, output_file_name,
    #                 min_subread_length=0)
    run_bam_to_fasta(input_file_name, output_file_name)
def task_prepare_falcon(self):
    i_fasta_fn = fn(self.fasta)
    input_fofn_fn = fn(self.input_fofn)
    fc_cfg_fn = fn(self.fc_cfg)
    fc_json_config_fn = fn(self.fc_json_config)
    config_falcon = self.parameters['falcon']
    run_prepare_falcon(config_falcon, i_fasta_fn, fc_cfg_fn, fc_json_config_fn, input_fofn_fn)
def task_falcon(self):
    fc_cfg_fn = fn(self.fc_cfg)
    fasta_fn = fn(self.asm_fasta)
    preads_fofn_fn = fn(self.preads_fofn)
    run_falcon(fc_cfg_fn, fasta_fn, preads_fofn_fn)
def task_report_pre_assembly(self):
    i_json_config_fn = fn(self.json_config)
    i_raw_reads_fofn_fn = fn(self.raw_reads_fofn)
    i_preads_fofn_fn = fn(self.preads_fofn)
    o_json_fn = fn(self.pre_assembly_report)
    report = run_report_pre_assembly(i_json_config_fn, i_raw_reads_fofn_fn, i_preads_fofn_fn, o_json_fn)
def task_fasta2referenceset(self):
    """Copied from pbsmrtpipe/pb_tasks/pacbio.py:run_fasta_to_referenceset()
    """
    input_file_name = fn(self.fasta)
    output_file_name = fn(self.referenceset)
    sys.system('rm -f {} {}'.format(
        output_file_name,
        input_file_name + '.fai'))
    cmd = 'dataset create --type ReferenceSet --generateIndices {} {}'.format(
            output_file_name, input_file_name)
    sys.system(cmd)
def task_pbalign_scatter(self):
    reads_fn = fn(self.dataset)
    referenceset_fn = fn(self.referenceset)
    out_json_fn = fn(self.out_json)
    run_pbalign_scatter(subreads=reads_fn, ds_reference=referenceset_fn, cjson_out=out_json_fn)
def task_pbalign_gather(self):
    ds_out_fn = fn(self.ds_out)
    dos = self.inputDataObjs
    dset_fns = [fn(v) for k,v in dos.items() if k.startswith('alignmentset')]
    run_pbalign_gather(dset_fns, ds_out_fn)
def task_pbalign(self):
    reads_fn = fn(self.dataset)
    referenceset_fn = fn(self.referenceset)
    alignmentset_fn = fn(self.alignmentset)
    task_opts = self.parameters['pbalign']
    options = task_opts.get('options', '')
    algorithmOptions = task_opts.get('algorithmOptions', '')
    run_pbalign(reads_fn, referenceset_fn, alignmentset_fn, options, algorithmOptions)
def task_gc_scatter(self):
    alignmentset_fn = fn(self.alignmentset)
    referenceset_fn = fn(self.referenceset)
    chunks_fofn_fn = fn(self.out_fofn)
    run_gc_scatter(alignmentset_fn, referenceset_fn, chunks_fofn_fn)
def task_gc_gather(self):
    ds_out_fn = fn(self.ds_out)
    dos = self.inputDataObjs
    dset_fns = [fn(v) for k,v in dos.items() if k.startswith('contigset')]
    run_gc_gather(dset_fns, ds_out_fn)
def task_gc_gather_fastq(self):
    fastq_out_fn = fn(self.fastq_out)
    dos = self.inputDataObjs
    dset_fns = [fn(v) for k,v in dos.items()]
    run_gc_gather_fastq(dset_fns, fastq_out_fn)
def task_genomic_consensus(self):
    alignmentset_fn = fn(self.alignmentset)
    referenceset_fn = fn(self.referenceset)
    polished_fastq_fn = fn(self.polished_fastq)
    variants_gff_fn = fn(self.variants_gff)
    consensus_contigset_fn = fn(self.consensus_contigset)
    #consensus_contigset_fn = "DUMMY"
    task_opts = self.parameters['variantCaller']
    options = task_opts.get('options', '')
    run_gc(alignmentset_fn, referenceset_fn, polished_fastq_fn, variants_gff_fn, consensus_contigset_fn, options)
def task_summarize_coverage(self):
    gathered_alignmentset_fn = fn(self.gathered_alignmentset)
    referenceset_fn = fn(self.referenceset)
    alignment_summary_gff_fn = fn(self.alignment_summary_gff)
    task_opts = self.parameters['pbreports.tasks.summarize_coverage']
    options = task_opts.get('options', '')
    run_summarize_coverage(gathered_alignmentset_fn, referenceset_fn, alignment_summary_gff_fn, options)
def task_polished_assembly_report(self):
    alignment_summary_gff_fn = fn(self.alignment_summary_gff)
    polished_fastq_fn = fn(self.polished_fastq)
    report_fn = fn(self.report_json)
    run_polished_assembly_report(alignment_summary_gff_fn, polished_fastq_fn, report_fn)
def task_foo(self):
    log.debug('WARNME1 {!r}'.format(__name__))
    #print repr(self.parameters), repr(self.URL), repr(self.foo1)
    sys.system('touch {}'.format(fn(self.foo2)))

def yield_pipeline_chunk_names_from_json(ifs, key):
    d = json.loads(ifs.read())
    for cs in d['chunks']:
        #chunk_id = cs['chunk_id']
        chunk_datum = cs['chunk']
        yield chunk_datum[key]
def create_tasks_pbalign(chunk_json_pfn, referenceset_pfn, parameters):
    """Create a pbalign task for each chunk, plus a gathering task.
    """
    tasks = list()
    alignmentsets = dict()
    for i, subreadset_fn in enumerate(sorted(yield_pipeline_chunk_names_from_json(open(fn(chunk_json_pfn)), '$chunk.subreadset_id'))):
        subreadset_pfn = makePypeLocalFile(subreadset_fn)
        alignmentset_pfn = makePypeLocalFile('align.subreads.{:02d}.alignmentset.xml'.format(i))
        alignmentsets['alignmentsets_{:02d}'.format(i)] = alignmentset_pfn
        """Also produces:
        aligned.subreads.alignmentset.bam
        aligned.subreads.alignmentset.bam.bai
        aligned.subreads.alignmentset.bam.pbi
        """
        make_task = PypeTask(
                inputs = {"chunk_json": chunk_json_pfn,
                          "dataset": subreadset_pfn,
                          "referenceset": referenceset_pfn,},
                outputs = {"alignmentset": alignmentset_pfn,},
                parameters = parameters,
                TaskType = PypeTaskBase,
                URL = "task://localhost/pbalign/{}".format(os.path.basename(subreadset_fn)))
        task = make_task(task_pbalign)
        tasks.append(task)
    alignmentset_pfn = makePypeLocalFile('aligned.subreads.alignmentset.xml')
    log.debug('alis:{}'.format(repr(alignmentsets)))
    make_task = PypeTask(
            inputs = alignmentsets,
            outputs = {"ds_out": alignmentset_pfn,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/pbalign_gather")
    task = make_task(task_pbalign_gather)
    tasks.append(task)
    return tasks, alignmentset_pfn
def mkdirs(d):
    log.debug('mkdir -p {}'.format(d))
    if not os.path.isdir(d):
        os.makedirs(d)
def create_tasks_gc(fofn_pfn, referenceset_pfn, parameters):
    """Create a gc task for each chunk, plus a gathering task.
    Here is the convoluted workflow:
    1. For each gc instance "chunk":
      A. variantCaller writes .fasta
      B. We create a contigset for the .fasta
    2. We keep the contigset output filenames in a FOFN (from run_gc_scatter)
       and pass that to run_gc_gather().
    3. We read each contigset and add them to a gathered ContigSet.
    4. We "consolidate" their underlying .fasta "resources",
       assuming their filenames match except extenion.
    5. Finally, we write the gathered contigset.
    Whew!
    We also gather fastq here, for convenience.
    """
    tasks = list()
    contigsets = dict()
    fastqs = dict()
    for i, alignmentset_fn in enumerate(open(fn(fofn_pfn)).read().split()):
        wdir = 'gc-{:02}'.format(i)
        mkdirs(wdir) # Assume CWD is correct.
        alignmentset_pfn = makePypeLocalFile(alignmentset_fn) # New pfn cuz it was not pfn before.
        polished_fastq_pfn = makePypeLocalFile(os.path.join(wdir, 'consensus.fastq'))
        variants_gff_pfn = makePypeLocalFile(os.path.join(wdir, 'variants.gff'))
        consensus_contigset_pfn = makePypeLocalFile(os.path.join(wdir, 'consensus.contigset.xml'))
        """Also produces:
        consensus.fasta
        consensus.fasta.fai

        And note that these files names are important, as pbcoretools gathering expects
        a particular pattern.
        """
        contigsets['contigset_{:02d}'.format(i)] = consensus_contigset_pfn
        fastqs['fastq_{:02d}'.format(i)] = polished_fastq_pfn
        make_task = PypeTask(
                inputs = {"alignmentset": alignmentset_pfn,
                          "referenceset": referenceset_pfn,},
                outputs = {
                    "polished_fastq": polished_fastq_pfn,
                    "variants_gff": variants_gff_pfn,
                    "consensus_contigset": consensus_contigset_pfn,
                },
                parameters = parameters,
                TaskType = PypeTaskBase,
                URL = "task://localhost/genomic_consensus/{}".format(os.path.basename(alignmentset_fn)))
        task = make_task(task_genomic_consensus)
        tasks.append(task)
    contigset_pfn = makePypeLocalFile('contigset.xml')
    log.debug('contigsets:{}'.format(repr(contigsets)))
    make_task = PypeTask(
            inputs = contigsets,
            outputs = {"ds_out": contigset_pfn, },
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/gc_gather")
    task = make_task(task_gc_gather)
    tasks.append(task)

    gathered_fastq_pfn = makePypeLocalFile("gathered.fastq")
    make_task = PypeTask(
            inputs = fastqs,
            outputs = {"fastq_out": gathered_fastq_pfn, },
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/gc_gather_fastq")
    task = make_task(task_gc_gather_fastq)
    tasks.append(task)

    return tasks, contigset_pfn, gathered_fastq_pfn

def flow(config):
    parameters = config
    #exitOnFailure=config['stop_all_jobs_on_failure'] # only matter for parallel jobs
    #wf.refreshTargets(exitOnFailure=exitOnFailure)
    #concurrent_jobs = config["pa_concurrent_jobs"]
    #PypeThreadWorkflow.setNumThreadAllowed(concurrent_jobs, concurrent_jobs)
    #wf = PypeThreadWorkflow()
    #wf = PypeWorkflow()
    #wf = PypeWorkflow(job_type='local')
    log.debug('config=\n{}'.format(pprint.pformat(config)))
    wf = PypeWorkflow(job_type=config['hgap']['job_type'])

    dataset_pfn = makePypeLocalFile(config['pbsmrtpipe']['input_files'][0])
    fdataset_pfn = makePypeLocalFile('filtered.subreadset.xml')
    orig_fasta_pfn = makePypeLocalFile('input.fasta')
    make_task = PypeTask(
            inputs = {"i_dataset": dataset_pfn,},
            outputs = {"o_dataset": fdataset_pfn,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/filterbam")
    task = make_task(task_filterbam)
    wf.addTask(task)
    wf.refreshTargets()

    make_task = PypeTask(
            inputs = {"dataset": fdataset_pfn, },
            outputs =  {"fasta": orig_fasta_pfn, },
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/bam2fasta")
    task = make_task(task_bam2fasta)
    wf.addTask(task)
    wf.refreshTargets()

    input_fofn_pfn = makePypeLocalFile('raw_reads.fofn')
    fc_cfg_pfn = makePypeLocalFile('fc.cfg')
    fc_json_config_pfn = makePypeLocalFile("fc.json")
    make_task = PypeTask(
            inputs = {"fasta": orig_fasta_pfn, },
            outputs = {"fc_cfg": fc_cfg_pfn,
                       "fc_json_config": fc_json_config_pfn,
                       "input_fofn": input_fofn_pfn, },
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/prepare_falcon")
    task = make_task(task_prepare_falcon)
    wf.addTask(task)
    wf.refreshTargets()

    # We could integrate the FALCON workflow here, but for now we will just execute it,
    # so we can repeat the sub-flow easily.
    asm_fasta_pfn = makePypeLocalFile('asm.fasta')
    preads_fofn_pfn = makePypeLocalFile('preads.fofn') # for the preassembly report
    make_task = PypeTask(
            inputs = {"fc_cfg": fc_cfg_pfn,},
            outputs = {"asm_fasta": asm_fasta_pfn,
                       "preads_fofn": preads_fofn_pfn, },
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/falcon")
    task = make_task(task_falcon)
    wf.addTask(task)

    pre_assembly_report_pfn = makePypeLocalFile("pre_assembly_report.json")
    make_task = PypeTask(
            inputs = {"json_config": fc_json_config_pfn,
                      "raw_reads_fofn": input_fofn_pfn,
                      "preads_fofn": preads_fofn_pfn, },
            outputs = {"pre_assembly_report": pre_assembly_report_pfn, },
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/report_pre_assembly")
    task = make_task(task_report_pre_assembly)
    wf.addTask(task)
    wf.refreshTargets()

    # The reset of the workflow will operate on datasets, not fasta directly.
    referenceset_pfn = makePypeLocalFile('asm.referenceset.xml')
    make_task = PypeTask(
            inputs =  {"fasta": asm_fasta_pfn,},
            outputs = {"referenceset": referenceset_pfn,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/fasta2referenceset")
    task = make_task(task_fasta2referenceset)
    wf.addTask(task)
    wf.refreshTargets()

    # scatter the subreads for pbalign
    """Produces:
    pbalign_chunk.json
    chunk_subreadset_*.subreadset.xml
    """
    pbalign_chunk_json_pfn = makePypeLocalFile('pbalign_chunk.json')
    make_task = PypeTask(
            inputs = {"dataset": dataset_pfn,
                      "referenceset": referenceset_pfn,},
            outputs = {"out_json": pbalign_chunk_json_pfn,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/pbalign_scatter")
    task = make_task(task_pbalign_scatter)
    wf.addTask(task)
    wf.refreshTargets()

    # After scattering, we can specify the pbalign jobs.
    tasks, alignmentset_pfn = create_tasks_pbalign(pbalign_chunk_json_pfn, referenceset_pfn, parameters)
    #wf.addTasks(tasks)
    for task in tasks:
        wf.addTask(task)
    wf.refreshTargets()

    # scatter the alignmentset for genomic_consensus (variantCaller)
    """Produces:
    gc.chunks.fofn
    ???*.congitset.xml ???
    """
    gc_chunks_fofn_pfn = makePypeLocalFile('gc.chunks.fofn')
    make_task = PypeTask(
            inputs = {"alignmentset": alignmentset_pfn,
                      "referenceset": referenceset_pfn,},
            outputs = {"out_fofn": gc_chunks_fofn_pfn,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/gc_scatter")
    task = make_task(task_gc_scatter)
    wf.addTask(task)
    wf.refreshTargets()

    tasks, contigset_pfn, gathered_fastq_pfn = create_tasks_gc(gc_chunks_fofn_pfn, referenceset_pfn, parameters)
    #wf.addTasks(tasks)
    for task in tasks:
        wf.addTask(task)
    wf.refreshTargets()


    # reports

    alignment_summary_gff_pfn = makePypeLocalFile("alignment_summary.gff")
    make_task = PypeTask(
            inputs = {"referenceset": referenceset_pfn,
                      "gathered_alignmentset": alignmentset_pfn,},
            outputs = {"alignment_summary_gff": alignment_summary_gff_pfn,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/summarize_coverage")
    task = make_task(task_summarize_coverage)
    wf.addTask(task)

    polished_assembly_report_json_pfn = makePypeLocalFile('polished_assembly_report.json')
    make_task = PypeTask(
            inputs = {"alignment_summary_gff": alignment_summary_gff_pfn,
                      "polished_fastq": gathered_fastq_pfn,},
            outputs = {"report_json": polished_assembly_report_json_pfn,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/polished_assembly_report")
    task = make_task(task_polished_assembly_report)
    wf.addTask(task)

    wf.refreshTargets()
    #return
    ##############

    if not os.path.exists('foo.bar1'):
        sys.system('touch foo.bar1')
    foo_fn1 = makePypeLocalFile('foo.bar1')
    foo_fn2 = makePypeLocalFile('foo.bar2')
    make_task = PypeTask(
            inputs = {"foo1": foo_fn1,},
            outputs =  {"foo2": foo_fn2,},
            parameters = parameters,
            TaskType = PypeTaskBase,
            URL = "task://localhost/foo")
    task = make_task(task_foo)
    wf.addTask(task)
    wf.refreshTargets()

    #raise Exception('hi')
"""
pbreports

pbreports.tasks.polished_assembly-0:
{
    "driver": {
        "env": {},
        "exe": "python -m pbreports.report.polished_assembly --resolved-tool-contract ",
        "serialization": "json"
    },
    "resolved_tool_contract": {
        "_comment": "Created by pbcommand v0.3.13",
        "input_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbreports.tasks.summarize_coverage-0/alignment_summary.gff",
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.gather_fastq-1/file.fastq"
        ],
        "is_distributed": true,
        "nproc": 1,
        "options": {},
        "output_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbreports.tasks.polished_assembly-0/polished_assembly_report.json"
        ],
        "resources": [],
        "task_type": "pbsmrtpipe.task_types.standard",
        "tool_contract_id": "pbreports.tasks.polished_assembly"
    }
}

pbreports.tasks.summarize_coverage-0:
{
    "driver": {
        "env": {},
        "exe": "python -m pbreports.report.summarize_coverage.summarize_coverage --resolved-tool-contract ",
        "serialization": "json"
    },
    "resolved_tool_contract": {
        "_comment": "Created by pbcommand v0.3.13",
        "input_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.gather_alignmentset-1/file.alignmentset.xml",
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.fasta2referenceset-0/file.referenceset.referenceset.xml"
        ],
        "is_distributed": true,
        "nproc": 1,
        "options": {
            "pbreports.task_options.force_num_regions": false,
            "pbreports.task_options.num_regions": 1000,
            "pbreports.task_options.region_size": 0
        },
        "output_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbreports.tasks.summarize_coverage-0/alignment_summary.gff"
        ],
        "resources": [],
        "task_type": "pbsmrtpipe.task_types.standard",
        "tool_contract_id": "pbreports.tasks.summarize_coverage"
    }
}

pbcoretools.tasks.subreadset_align_scatter-1:
{
    "driver": {
        "env": {},
        "exe": "python -m pbcoretools.tasks.scatter_subread_reference --resolved-tool-contract ",
        "serialization": "json"
    },
    "resolved_tool_contract": {
        "_comment": "Created by pbcommand v0.3.13",
        "chunk_keys": "chunk-key",
        "input_files": [
            "/pbi/collections/315/3150057/r54006_20160119_011818/2_B01/m54006_160119_063009.subreadset.xml",
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.fasta2referenceset-0/file.referenceset.referenceset.xml"
        ],
        "is_distributed": true,
        "max_nchunks": 24,
        "nproc": 1,
        "options": {
            "pbcoretools.task_options.scatter_subread_max_nchunks": 5
        },
        "output_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.subreadset_align_scatter-1/chunk.json"
        ],
        "resources": [],
        "task_type": "pbsmrtpipe.task_types.scattered",
        "tool_contract_id": "pbcoretools.tasks.subreadset_align_scatter"
    }
}

pbcoretools.tasks.gather_alignmentset-1:
{
    "driver": {
        "env": {},
        "exe": "python -m pbcoretools.tasks.gather_alignments --resolved-tool-contract ",
        "serialization": "json"
    },
    "resolved_tool_contract": {
        "_comment": "Created by pbcommand v0.3.13",
        "chunk_key": "$chunk.alignmentset_id",
        "input_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/.pbcoretools.tasks.subreadset_align_scatter-f4a69d33-00fa-46c6-a8f1-a5e05d5b80ca-gathered-pipeline.chunks.json"
        ],
        "is_distributed": false,
        "nproc": 1,
        "options": {},
        "output_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.gather_alignmentset-1/file.alignmentset.xml"
        ],
        "resources": [],
        "task_type": "pbsmrtpipe.task_types.gathered",
        "tool_contract_id": "pbcoretools.tasks.gather_alignmentset"
    }
}

pbcoretools.tasks.alignment_contig_scatter-1:
{
    "driver": {
        "env": {},
        "exe": "python -m pbcoretools.tasks.scatter_alignments_reference --resolved-tool-contract ",
        "serialization": "json"
    },
    "resolved_tool_contract": {
        "_comment": "Created by pbcommand v0.3.13",
        "chunk_keys": "chunk-key",
        "input_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.gather_alignmentset-1/file.alignmentset.xml",
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.fasta2referenceset-0/file.referenceset.referenceset.xml"
        ],
        "is_distributed": true,
        "max_nchunks": 24,
        "nproc": 1,
        "options": {
            "pbcoretools.task_options.scatter_alignments_reference_max_nchunks": 12
        },
        "output_files": [
            "/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks/pbcoretools.tasks.alignment_contig_scatter-1/chunk.json"
        ],
        "resources": [],
        "task_type": "pbsmrtpipe.task_types.scattered",
        "tool_contract_id": "pbcoretools.tasks.alignment_contig_scatter"
    }
}

also:
genomic_consensus.tasks.gff2vcf-0
genomic_consensus.tasks.gff2bed-0
pbcoretools.tasks.gather_gff-1
pbcoretools.tasks.gather_fastq-1
pbcoretools.tasks.gather_contigset-1

/pbi/dept/secondary/siv/smrtlink/smrtlink-alpha/smrtsuite_170220/userdata/jobs_root/000/000114/tasks
"""
