from __future__ import absolute_import
from falcon_polish.sys import symlink, cd, say, system, filesize
from pbcore.io import FastaIO
from pbcommand.engine import run_cmd as pb_run_cmd
from falcon_kit import run_support as support
from . import report_preassembly
import falcon_kit.functional
import falcon_kit.io
import falcon_kit.pype
import falcon_kit.pype_tasks
import pypeflow.do_task
import glob
import hashlib
import itertools
import json
import logging
import os
import re
import StringIO
import sys

log = logging.getLogger(__name__)


def run_cmd(cmd, *args, **kwds):
    errfile = os.path.abspath('pbfalcon.run_cmd.err')
    os.environ['PBFALCON_ERRFILE'] = errfile
    say('RUN: %s' %repr(cmd))
    rc = pb_run_cmd(cmd, *args, **kwds)
    say(' RC: %s' %repr(rc))
    if rc.exit_code:
        msg = repr(rc)
        if os.path.exists(errfile):
            with open(errfile) as ifs:
                msg += '\n' + ifs.read()
        raise Exception(msg)

def _get_config(fn):
    """Return a dict.
    """
    return support.get_config(support.parse_config(fn))

def _get_config_from_json_fileobj(ifs_json):
    i_json = ifs_json.read()
    say('JSON=\n%s' %i_json[:1024]) # truncated
    return json.loads(i_json)

def run_falcon_config_get_fasta(input_files, output_files):
        i_config_fn, = input_files
        o_fofn_fn, = output_files
        config = _get_config(i_config_fn)
        i_fofn_fn = config['input_fofn']
        if not os.path.isabs(i_fofn_fn):
            i_fofn_fn = os.path.join(os.path.dirname(i_config_fn), i_fofn_fn)
        msg = '%r -> %r' %(i_fofn_fn, o_fofn_fn)
        say(msg)
        with cd(os.path.dirname(i_fofn_fn)):
            return support.make_fofn_abs(i_fofn_fn, o_fofn_fn)
        return 0

def run_falcon_config(input_files, output_files):
        i_config_fn, i_fasta_fofn = input_files
        o_json_fn, = output_files
        log.info('i_config_fn cont: "{}"'.format(open(i_config_fn).read()))
        config = _get_config(i_config_fn)
        config['input_fofn'] = os.path.abspath(i_fasta_fofn)
        config['original_self'] = i_config_fn
        output = json.dumps(config, sort_keys=True, indent=4, separators=(',', ': '))
        out = StringIO.StringIO()
        out.write(output)
        log.info('falcon_config:\n' + output)
        with open(o_json_fn, 'w') as ofs:
            ofs.write(output)
        #return run_cmd('echo hi', open(hello, 'w'), sys.stderr, shell=False)
        return 0

def run_falcon_make_fofn_abs(input_files, output_files):
        i_json_fn, = input_files
        o_fofn_fn, = output_files
        config = _get_config_from_json_fileobj(open(i_json_fn))
        i_fofn_fn = config['input_fofn']
        if not i_fofn_fn.startswith('/'):
            # i_fofn_fn can be relative to the location of the config file.
            original_config_fn = config['original_self']
            i_fofn_fn = os.path.join(os.path.dirname(original_config_fn), i_fofn_fn)
        msg = 'run_falcon_make_fofn_abs(%r -> %r)' %(i_fofn_fn, o_fofn_fn)
        say(msg)
        with cd(os.path.dirname(i_fofn_fn)):
            return support.make_fofn_abs(i_fofn_fn, o_fofn_fn)
        return 0

def read_fns(fofn):
    print('read from fofn:%s' %repr(fofn))
    with open(fofn) as ifs:
        return ifs.read().split()
def write_fns(fofn, fns):
    """Remember the trailing newline, for fasta2DB.
    """
    print('write to fofn:%s' %repr(fofn))
    with open(fofn, 'w') as ofs:
        return ofs.write('\n'.join(list(fns) + ['']))

def run_generic_chunkable_jobs(input_files, output_files):
    units_of_work_fn, bash_template_fn, = input_files
    results_fn, = output_files
    run(
            script=falcon_kit.pype.TASK_GENERIC_RUN_UNITS_SCRIPT,
            inputs={
                'units_of_work': units_of_work_fn,
                'bash_template': bash_template_fn,
            },
            outputs={
                'results': results_fn,
            },
            parameters={},
    )
    return 0

def run(script, inputs, outputs, parameters):
    pypeflow.do_task.run_bash(script, inputs, outputs, parameters)

def run_falcon_build_rdb(input_files, output_files):
    i_general_config_fn, i_fofn_fn = input_files
    run_jobs_fn, db_fn, job_done_fn, length_cutoff_fn = output_files
    run(
            script=falcon_kit.pype_tasks.TASK_BUILD_RDB_SCRIPT,
            inputs={
                'config': i_general_config_fn,
                'raw_reads_fofn': i_fofn_fn,
            },
            outputs={
                'run_jobs': run_jobs_fn,
                'raw_reads_db': 'raw_reads.db',
                'length_cutoff': length_cutoff_fn,
                'db_build_done': job_done_fn,
            },
            parameters={},
    )
    #if not job_descs:
    #    raise Exception("No daligner jobs generated in '%s' by '%s'." %(run_daligner_jobs_fn, script_fn))
    # TODO: Add 0-jobs check to tool.

    # To use this filename in pb, we might need to add Dazzler FileType. symlink is simpler.
    symlink('raw_reads.db', db_fn)
    return 0

def run_falcon_build_pdb(input_files, output_files):
    i_general_config_fn, i_fofn_fn, = input_files
    run_jobs_fn, db_fn, job_done_fn, = output_files
    run(
            script=falcon_kit.pype_tasks.TASK_BUILD_PDB_SCRIPT,
            inputs={
                'config': i_general_config_fn,
                'preads_fofn': i_fofn_fn,
            },
            outputs={
                'run_jobs': run_jobs_fn,
                'preads_db': 'preads.db',
                'db_build_done': job_done_fn,
            },
            parameters={},
    )
    # To use this filename in pb, we might need to add Dazzler FileType. symlink is simpler.
    symlink('preads.db', db_fn)
    return 0

def run_daligner_split(input_files, output_files, db_prefix='raw_reads'):
    run_jobs_fn, rawreads_db_fn = input_files
    daligner_all_units_fn, daligner_bash_template_fn = output_files
    params = dict() #dict(parameters)
    params['db_prefix'] = db_prefix
    params['pread_aln'] = 0
    params['skip_checks'] = 0 #int(config.get('skip_checks', 0))
    params['wildcards'] = 'dal0_id'
    run(
        script=falcon_kit.pype_tasks.TASK_DALIGNER_SPLIT_SCRIPT,
        inputs={
            'run_jobs': run_jobs_fn,
            'db': rawreads_db_fn,
        },
        outputs={
            'split': daligner_all_units_fn,
            'bash_template': daligner_bash_template_fn,
        },
        parameters=params,
    )
    return 0

def run_daligner_find_las(input_files, output_files):
    gathered_fn, = input_files
    gathered_las_fn, = output_files
    run(
        script=falcon_kit.pype_tasks.TASK_DALIGNER_FIND_LAS_SCRIPT,
        inputs={'gathered': gathered_fn,
        },
        outputs={'las_paths': gathered_las_fn,
        },
        parameters={},
    )
    return 0

def run_las_merge_split(input_files, output_files, db_prefix='raw_reads'):
    run_jobs_fn, gathered_las_fn = input_files
    las_merge_all_units_fn, bash_template_fn = output_files
    params = dict()
    params['db_prefix'] = db_prefix
    params['wildcards'] = 'mer0_id'
    run(
        script=falcon_kit.pype_tasks.TASK_LAS_MERGE_SPLIT_SCRIPT,
        inputs={
            'run_jobs': run_jobs_fn,
            'gathered_las': gathered_las_fn,
        },
        outputs={
            'split': las_merge_all_units_fn,
            'bash_template': bash_template_fn,
        },
        parameters=params,
    )
    return 0

def run_las_merge_post_gather(input_files, output_files):
    gathered_fn, = input_files
    las_fofn_fn, p_id2las_fn, = output_files
    run(
        script=falcon_kit.pype_tasks.TASK_LAS_MERGE_GATHER_SCRIPT,
        inputs={'gathered': gathered_fn,
        },
        outputs={'p_id2las': p_id2las_fn,
                 'las': las_fofn_fn,
        },
        parameters={},
    )
    return 0

def run_cns_split(input_files, output_files):
    p_id2las_fn, raw_reads_db_fn, general_config_fn, length_cutoff_fn, = input_files
    all_units_of_work_fn, bash_template_fn, = output_files
    raw_reads_db_fn = os.path.realpath(raw_reads_db_fn)
    params = dict()
    params['wildcards'] = 'cns0_id,cns0_id2'
    run(
        script=falcon_kit.pype_tasks.TASK_CONSENSUS_SPLIT_SCRIPT,
        inputs={
            'p_id2las': p_id2las_fn,
            'raw_reads_db': raw_reads_db_fn,
            'length_cutoff': length_cutoff_fn,
            'config': general_config_fn,
        },
        outputs={
            'split': all_units_of_work_fn,
            'bash_template': bash_template_fn,
        },
        parameters=params,
    )
    return 0

def run_cns_post_gather(input_files, output_files):
    gathered_fn, = input_files
    preads_fofn_fn, = output_files
    run(
        script=falcon_kit.pype_tasks.TASK_CONSENSUS_GATHER_SCRIPT,
        inputs={
            'gathered': gathered_fn,
        },
        outputs={
            'preads_fofn': preads_fofn_fn,
        },
        parameters={},
    )
    return 0

def run_db2falcon(input_files, output_files):
    i_p_id2las_fn, i_preads_db_fn, = input_files
    o_preads4falcon_fasta_fn, o_job_done_fn, = output_files
    #db2falcon_dir = os.path.join(pread_dir, 'db2falcon')
    #db2falcon_done_fn = os.path.join(db2falcon_dir, 'db2falcon_done')
    #preads4falcon_fn = os.path.join(db2falcon_dir, 'preads4falcon.fasta')
    i_preads_db_fn = os.path.realpath(i_preads_db_fn)
    run(
        script=falcon_kit.pype_tasks.TASK_RUN_DB_TO_FALCON_SCRIPT,
        inputs={'p_id2las': i_p_id2las_fn,
                'preads_db': i_preads_db_fn,
                },
        outputs={'job_done': o_job_done_fn,
                 'preads4falcon': o_preads4falcon_fasta_fn,
                 },
        parameters={},
    )
    return 0

def run_falcon_asm(input_files, output_files):
    # Given, las_fofn.json and preads4falcon.fasta,
    # write preads.ovl and fastas (primary ctgs, etc.).
    i_preads4falcon_fasta_fn, i_preads_db_fn, i_las_fofn_fn, i_general_config_fn, i_db2falcon_done_fn, = input_files
    o_fasta_fn, = output_files
    assert_nonzero(i_preads4falcon_fasta_fn)
    assert_nonzero(i_las_fofn_fn) # useless test now; json is always non-empty
    j = falcon_kit.io.deserialize(i_las_fofn_fn)
    assert j, '{!r} must have at least one las file.'.format(i_las_fofn_fn)
    i_preads_db_fn = os.path.realpath(i_preads_db_fn)
    falcon_asm_done_fn = 'falcon_asm_done' #os.path.join(falcon_asm_dir, 'falcon_asm_done')
    parameters = dict()
    config = falcon_kit.io.deserialize(i_general_config_fn)
    for key in ('overlap_filtering_setting', 'length_cutoff_pr', 'fc_ovlp_to_graph_option'):
        parameters[key] = config[key]
    run(
        script=falcon_kit.pype_tasks.TASK_RUN_FALCON_ASM_SCRIPT,
        inputs={'db2falcon_done': i_db2falcon_done_fn,
                'db_file': i_preads_db_fn,
                'preads4falcon_fasta': i_preads4falcon_fasta_fn,
                'las_fofn': i_las_fofn_fn,
                'config': i_general_config_fn,
                },
        outputs={'falcon_asm_done': falcon_asm_done_fn},
        parameters=parameters,
    )
    p_ctg = 'p_ctg.fa'
    assert_nonzero(p_ctg)
    #symlink(p_ctg, o_fasta_fn)
    n_records = _linewrap_fasta(p_ctg, o_fasta_fn)
    if n_records == 0:
        # We already checked 0-length, but maybe this is still possible.
        # Really, we want to detect 0 base-length, but I do not know how yet.
        raise Exception("No records found in primary contigs: '%s'" %os.path.abspath(p_ctg))
    return 0

def run_rm_las(input_files, output_files, prefix):
    """ Delete all intermediate las files. """
    cmd = "pwd && find .. -type f -name '{}*.las' -delete -print".format(prefix)
    say(cmd)
    run_cmd(cmd, sys.stdout, sys.stderr)
    with open(output_files[0], 'w') as writer:
        writer.write("#%s" % cmd)
    return 0


def run_hgap(input_files, output_files, tmpdir):
    i_cfg_fn, i_logging_fn, i_subreadset_fn = input_files
    o_preads_fasta_fn, \
    o_polished_fasta_fn, o_polished_fastq_fn, o_polished_csv_fn, \
    o_aligned_subreads_fn, o_alignment_summary_gff_fn, o_unmapped_subreads_txt_fn, \
    o_contigset_fn, o_preass_json_fn, o_polass_json_fn, o_log_fn, = output_files
    # Update the logging-cfg with our log-file.
    logging_cfg = json.loads(open(i_logging_fn).read())
    logging_cfg['handlers']['handler_file_all']['filename'] = o_log_fn
    logging_fn = 'logging.json'
    with open(logging_fn, 'w') as ofs:
        ofs.write(json.dumps(logging_cfg))
    # Update the cfg with our subreadset. (Inside hgap_run?)
    # Run pypeflow.hgap.main.
    cmd = 'TMPDIR={tmpdir} python -m pbfalcon.cli.hgap_run --logging {logging_fn} {i_cfg_fn}'.format(**locals())
    system(cmd)
    # Write Reports
    with open('run-falcon/0-rawreads/report/pre_assembly_stats.json') as stats_ifs: # by convention
        with open(o_preass_json_fn, 'w') as report_ofs:
            report_preassembly.write_report_from_stats(stats_ifs, report_ofs)
    # Symlink expected outputs, by convention.
    symlink('run-falcon/1-preads_ovl/db2falcon/preads4falcon.fasta', o_preads_fasta_fn)
    symlink('run-gc-gather/contigset.fasta', o_polished_fasta_fn)
    symlink('run-gc-gather/gathered.fastq', o_polished_fastq_fn)
    symlink('run-polished-assembly-report/polished_coverage_vs_quality.csv', o_polished_csv_fn)
    symlink('run-polished-assembly-report/alignment.summary.gff', o_alignment_summary_gff_fn)
    symlink('run-pbalign_gather/aligned.subreads.alignmentset.xml', o_aligned_subreads_fn)
    symlink('run-pbalign_gather/unmapped.txt', o_unmapped_subreads_txt_fn)
    symlink('run-gc-gather/contigset.xml', o_contigset_fn)
    symlink('run-polished-assembly-report/polished_assembly_report.json', o_polass_json_fn)
    return 0

def run_stats_preassembly_yield(input_files, output_files):
    #pre_assembly_report_fn = os.path.join(rdir, 'pre_assembly_stats.json')
    i_general_config_fn, i_preads_fofn_fn, i_raw_reads_db_fn, i_length_cutoff_fn, = input_files
    o_stats_json_fn, = output_files
    raw_reads_db_fn = os.path.realpath(i_raw_reads_db_fn)
    config = falcon_kit.io.deserialize(i_general_config_fn)
    params = dict() #dict(parameters)
    params['length_cutoff_user'] = config['length_cutoff']
    params['genome_length'] = config['genome_size'] # note different name; historical
    run(
        script=falcon_kit.pype_tasks.TASK_REPORT_PRE_ASSEMBLY_SCRIPT,
        inputs={'length_cutoff': i_length_cutoff_fn,
                'raw_reads_db': raw_reads_db_fn,
                'preads_fofn': i_preads_fofn_fn,
                'config': i_general_config_fn,
        },
        outputs={'pre_assembly_report': o_stats_json_fn,
        },
        parameters=params,
    )

def run_report_preassembly_yield(input_files, output_files):
    i_general_config_fn, i_preads_fofn_fn, i_raw_reads_db_fn, i_length_cutoff_fn = input_files
    o_json_fn, = output_files
    i_raw_reads_db_fn = os.path.realpath(i_raw_reads_db_fn)
    # Ignore length_cutff_fn, for now.
    kwds = {
        'i_json_config_fn': i_general_config_fn,
        'i_raw_reads_db_fn': i_raw_reads_db_fn,
        'i_preads_fofn_fn': i_preads_fofn_fn,
        'o_json_fn': o_json_fn,
    }
    report_preassembly.for_task(**kwds)
    return 0


def _linewrap_fasta(ifn, ofn):
    """For the pbsmrtpipe validator.
    Not sure whether any tools actually require this.
    """
    n = 0
    with FastaIO.FastaReader(ifn) as fa_in:
        with FastaIO.FastaWriter(ofn) as fa_out:
            for rec in fa_in:
                n += 1
                fa_out.writeRecord(rec)
    return n

def assert_nonzero(fn):
    if filesize(fn) == 0:
        raise Exception("0-length filesize for: '%s'" %os.path.abspath(fn))
