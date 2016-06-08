from __future__ import absolute_import
from . import report_preassembly
from .sys import symlink, cd, say, system, filesize
from pbcore.io import FastaIO
from pbcommand.engine import run_cmd as pb_run_cmd
from falcon_kit import run_support as support
from falcon_kit.mains import run as support2
import falcon_kit.functional
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
    say('RUN: %s' %repr(cmd))
    rc = pb_run_cmd(cmd, *args, **kwds)
    say(' RC: %s' %repr(rc))
    if rc.exit_code:
        raise Exception(repr(rc))

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

def run_falcon_build_rdb(input_files, output_files):
    print('output_files: %s' %(repr(output_files)))
    cwd = os.getcwd()
    odir = os.path.realpath(os.path.abspath(os.path.dirname(output_files[0])))
    if True: #debug
        if cwd != odir:
            raise Exception('%r != %r' %(cwd, odir))
    i_json_config_fn, i_fofn_fn = input_files
    print('output_files: %s' %repr(output_files))
    run_daligner_jobs_fn, job_done_fn = output_files
    config = _get_config_from_json_fileobj(open(i_json_config_fn))
    script_fn = os.path.join(odir, 'prepare_rdb.sh') # implies run-dir too
    #job_done_fn = os.path.join(odir, 'job.done') # not needed in pbsmrtpipe today tho
    support.build_rdb(i_fofn_fn, config, job_done_fn, script_fn, run_daligner_jobs_fn)
    run_cmd('bash %s' %script_fn, sys.stdout, sys.stderr, shell=False)
    job_descs = falcon_kit.functional.get_daligner_job_descriptions(open(run_daligner_jobs_fn), 'raw_reads')
    if not job_descs:
        raise Exception("No daligner jobs generated in '%s' by '%s'." %(run_daligner_jobs_fn, script_fn))
    return 0

def run_daligner_jobs(input_files, output_files, db_prefix='raw_reads'):
    print('run_daligner_jobs: %s %s' %(repr(input_files), repr(output_files)))
    i_json_config_fn, run_daligner_job_fn, = input_files
    o_fofn_fn, = output_files
    db_dir = os.path.dirname(run_daligner_job_fn)
    cmds = ['pwd', 'ls -al']
    fns = ['.{pre}.bps', '.{pre}.idx', '{pre}.db']
    cmds += [r'\rm -f %s' %fn for fn in fns]
    cmds += ['ln -sf {dir}/%s .' %fn for fn in fns]
    cmd = ';'.join(cmds).format(
            dir=os.path.relpath(db_dir), pre=db_prefix)
    run_cmd(cmd, sys.stdout, sys.stderr, shell=True)
    cwd = os.getcwd()
    config = _get_config_from_json_fileobj(open(i_json_config_fn))
    tasks = create_daligner_tasks(
            run_daligner_job_fn, cwd, db_prefix, db_prefix+'.db', config)
    odirs = []
    for jobd, args in tasks.items():
        with cd(jobd):
            support.run_daligner(**args)
            script_fn = args['script_fn']
            run_cmd('bash %s' %script_fn, sys.stdout, sys.stderr, shell=False)
            odirs.append(os.path.dirname(script_fn))
    write_fns(o_fofn_fn, itertools.chain.from_iterable(glob.glob('%s/*.las' %d) for d in odirs))
    return 0

#    def scripts_daligner(run_jobs_fn, db_prefix, rdb_build_done, pread_aln=False):
def create_daligner_tasks(run_jobs_fn, wd, db_prefix, db_file, config, pread_aln = False):
    tasks = dict() # uid -> parameters-dict

    nblock = support.get_nblock(db_file)

    re_daligner = re.compile(r'\bdaligner\b')

    line_count = 0
    job_descs = falcon_kit.functional.get_daligner_job_descriptions(open(run_jobs_fn), db_prefix)
    if not job_descs:
        raise Exception("No daligner jobs generated in '%s'." %run_jobs_fn)
    for desc, bash in job_descs.iteritems():
        job_uid = '%08d' %line_count
        line_count += 1
        jobd = os.path.join(wd, "./job_%s" % job_uid)
        support.make_dirs(jobd)
        call = "cd %s; ln -sf ../.%s.bps .; ln -sf ../.%s.idx .; ln -sf ../%s.db ." % (jobd, db_prefix, db_prefix, db_prefix)
        rc = os.system(call)
        if rc:
            raise Exception("Failure in system call: %r -> %d" %(call, rc))
        job_done = os.path.abspath("%s/job_%s_done" %(jobd, job_uid))
        if pread_aln:
            bash = re_daligner.sub("daligner_p", bash)
        bash += """
rm -f *.C?.las
rm -f *.N?.las
rm -f *.C?.S.las
rm -f *.N?.S.las
"""
        script_fn = os.path.join(jobd , "rj_%s.sh"% (job_uid)) # also implies run-dir
        args = {
            'daligner_script': bash,
            'db_prefix': db_prefix,
            'config': config,
            'job_done': job_done,
            'script_fn': script_fn,
        }
        daligner_task = args #make_daligner_task( task_run_daligner )
        tasks[jobd] = daligner_task
    return tasks

def run_merge_consensus_jobs(input_files, output_files, db_prefix='raw_reads'):
    print('run_merge_consensus_jobs: %s %s %s' %(db_prefix, repr(input_files), repr(output_files)))
    i_json_config_fn, run_daligner_job_fn, i_fofn_fn = input_files
    o_fofn_fn, = output_files
    db_dir = os.path.dirname(run_daligner_job_fn)
    cmds = ['pwd', 'ls -al']
    fns = ['.{pre}.bps', '.{pre}.idx', '{pre}.db']
    cmds += ['rm -f %s' %fn for fn in fns]
    cmds += ['ln -sf {dir}/%s .' %fn for fn in fns]
    cmd = ';'.join(cmds).format(
            dir=os.path.relpath(db_dir), pre=db_prefix)
    run_cmd(cmd, sys.stdout, sys.stderr, shell=True)
    cwd = os.getcwd() # same as dir of o_fofn_fn
    config = _get_config_from_json_fileobj(open(i_json_config_fn))
    # i_fofn_fn has the .las files, so create_merge_tasks does not need to look for theme.
    tasks = create_merge_tasks(i_fofn_fn, run_daligner_job_fn, cwd, db_prefix=db_prefix, config=config)

    las_fns = _run_merge_jobs(
            dict((p_id, (argstuple[0], argstuple[2])) for (p_id, argstuple) in tasks.items()))

    if db_prefix == 'raw_reads':
        fasta_fns = _run_consensus_jobs(
            dict((p_id, (argstuple[1], argstuple[3])) for (p_id, argstuple) in tasks.items()))
        # Record '*.fasta' in FOFN.
        write_fns(o_fofn_fn, sorted(os.path.abspath(f) for f in fasta_fns))
        assert_nonzero(o_fofn_fn)
        return

    # Record '*.las' from merge_jobs in FOFN.
    write_fns(o_fofn_fn, sorted(os.path.abspath(f) for f in las_fns))

    # Generate preads4falcon.fasta from preads.db
    script_fn = os.path.join(cwd, "run_db2falcon.sh")
    job_done = script_fn + '_done'
    args = {
        'config': config,
        'job_done': job_done,
        'script_fn': script_fn,
    }
    support.run_db2falcon(**args)
    run_cmd('bash %s' %script_fn, sys.stdout, sys.stderr, shell=False)
    assert_nonzero('preads4falcon.fasta')
    assert_nonzero(o_fofn_fn)
    return 0

merged_las_fofn_bfn = 'merged_las.fofn'
#DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

def _run_merge_jobs(tasks):
    fns = list()
    for p_id, (merge_args, las_bfn) in tasks.items():
            run_dir = merge_args['merge_subdir']
            job_done = "merge_%05d_done" %p_id
            script_fn = os.path.join(run_dir, "merge_%05d.sh" % (p_id))
            merge_args['job_done'] = job_done
            merge_args['script_fn'] = script_fn
            del merge_args['merge_subdir'] # was just a temporary hack
            support.run_las_merge(**merge_args)
            run_cmd('bash %s' %script_fn, sys.stdout, sys.stderr, shell=False)
            fns.append(os.path.join(run_dir, las_bfn))
    return fns # *.las

def _run_consensus_jobs(tasks):
    fns = list()
    for p_id, (cons_args, fasta_bfn) in tasks.items():
            run_dir = 'preads'
            job_done = "c_%05d_done" %p_id
            script_fn = os.path.join(run_dir, "c_%05d.sh" %(p_id))
            cons_args['job_done'] = job_done
            cons_args['script_fn'] = script_fn
            support.run_consensus(**cons_args)
            run_cmd('bash %s' %script_fn, sys.stdout, sys.stderr, shell=False)
            fns.append(os.path.join(run_dir, fasta_bfn))
    return fns # *.fasta

def create_merge_tasks(i_fofn_fn, run_jobs_fn, wd, db_prefix, config):
    #merge_scripts = bash.scripts_merge(config, db_prefix, run_jobs_fn)
    tasks = {} # pid -> (merge_params, cons_params)
    mjob_data = {}

    with open(run_jobs_fn) as f :
        for l in f:
            l = l.strip().split()
            if l[0] not in ( "LAsort", "LAmerge", "mv" ):
                continue
            if l[0] == "LAsort":
                # We now run this part w/ daligner, but we still need
                # a small script for some book-keeping.
                p_id = int( l[2].split(".")[1] )
                mjob_data.setdefault( p_id, [] )
                #mjob_data[p_id].append(  " ".join(l) ) # Already done w/ daligner!
            if l[0] == "LAmerge":
                l2 = l[2].split(".")
                if l2[1][0] == "L":
                    p_id = int(  l[2].split(".")[2] )
                    mjob_data.setdefault( p_id, [] )
                    mjob_data[p_id].append(  " ".join(l) )
                else:
                    p_id = int( l[2].split(".")[1] )
                    mjob_data.setdefault( p_id, [] )
                    mjob_data[p_id].append(  " ".join(l) )
            if l[0] == "mv":
                l2 = l[1].split(".")
                if l2[1][0] == "L":
                    p_id = int(  l[1].split(".")[2] )
                    mjob_data.setdefault( p_id, [] )
                    mjob_data[p_id].append(  " ".join(l) )
                else:
                    p_id = int( l[1].split(".")[1] )
                    mjob_data.setdefault( p_id, [] )
                    mjob_data[p_id].append(  " ".join(l) )

    # Could be L1.* or preads.*
    re_las = re.compile(r'\.(\d*)(\.\d*)?\.las$')

    for p_id in mjob_data:
        s_data = mjob_data[p_id]

        support.make_dirs("%s/preads" % (wd) )
        support.make_dirs("%s/las_files" % (wd) )
        merge_subdir = "m_%05d" %p_id
        merge_dir = os.path.join(wd, merge_subdir)
        support.make_dirs(merge_dir)
        #merge_script_file = os.path.abspath( "%s/m_%05d/m_%05d.sh" % (wd, p_id, p_id) )
        merge_script = StringIO.StringIO()
        with cd(merge_dir):
            print("i_fofn_fn=%r" %i_fofn_fn)
            # Since we could be in the gather-task-dir, instead of globbing,
            # we will read the fofn.
            for fn in open(i_fofn_fn).read().splitlines():
                basename = os.path.basename(fn)
                mo = re_las.search(basename)
                if not mo:
                    continue
                left_block = int(mo.group(1))
                if left_block != p_id:
                    # By convention, m_00005 merges L1.5.*.las, etc.
                    continue
                symlink(fn)

        for l in s_data:
            print >> merge_script, l
        las_bfn = '%s.%d.las' %(db_prefix, p_id)
        #print >> merge_script, 'echo %s >| %s' %(las_bfn, merged_las_fofn_bfn)

        #job_done = makePypeLocalFile(os.path.abspath( "%s/m_%05d/m_%05d_done" % (wd, p_id, p_id)  ))
        parameters =  {"script": merge_script.getvalue(),
                       "merge_subdir": merge_subdir,
                       "config": config}
        merge_task = parameters

        fasta_bfn = "out.%05d.fasta" %p_id
        out_file_fn = os.path.abspath("%s/preads/%s" %(wd, fasta_bfn))
        #out_done = makePypeLocalFile(os.path.abspath( "%s/preads/c_%05d_done" % (wd, p_id)  ))
        parameters =  {
                       "db_fn": '../{}'.format(db_prefix),
                       "las_fn": '../{}/{}'.format(merge_subdir, las_bfn), # assuming merge ran in merge_dir
                       "out_file_fn": out_file_fn,
                       #"out_done": out_done,
                       "config": config}
        cons_task = parameters
        tasks[p_id] = (merge_task, cons_task, las_bfn, fasta_bfn)

    return tasks

def run_falcon_build_pdb(input_files, output_files):
    print('output_files: %s' %(repr(output_files)))
    cwd = os.getcwd()
    odir = os.path.realpath(os.path.abspath(os.path.dirname(output_files[0])))
    if True: #debug
        if cwd != odir:
            raise Exception('%r != %r' %(cwd, odir))
    i_json_config_fn, i_fofn_fn = input_files
    print('output_files: %s' %repr(output_files))
    run_daligner_jobs_fn, = output_files
    config = _get_config_from_json_fileobj(open(i_json_config_fn))
    script_fn = os.path.join(odir, 'prepare_pdb.sh')
    job_done_fn = os.path.join(odir, 'job_done')
    support.build_pdb(i_fofn_fn, config, job_done_fn, script_fn, run_daligner_jobs_fn)
    run_cmd('bash %s' %script_fn, sys.stdout, sys.stderr, shell=False)
    job_descs = falcon_kit.functional.get_daligner_job_descriptions(open(run_daligner_jobs_fn), 'preads')
    if not job_descs:
        raise Exception("No daligner jobs generated in '%s' by '%s'." %(run_daligner_jobs_fn, script_fn))
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

def run_falcon_asm(input_files, output_files):
    i_json_config_fn, i_fofn_fn = input_files
    o_fasta_fn, = output_files
    cwd = os.getcwd()
    pread_dir = os.path.dirname(i_fofn_fn)
    preads4falcon_fasta_fn = os.path.join(pread_dir, 'preads4falcon.fasta')
    db_file = os.path.join(pread_dir, 'preads.db')
    job_done = os.path.join(cwd, 'job_done')
    config = _get_config_from_json_fileobj(open(i_json_config_fn))
    script_fn = os.path.join(cwd ,"run_falcon_asm.sh")
    args = {
        'las_fofn_fn': i_fofn_fn,
        'preads4falcon_fasta_fn': preads4falcon_fasta_fn,
        'db_file_fn': db_file,
        'config': config,
        'job_done': job_done,
        'script_fn': script_fn,
    }
    assert_nonzero(i_fofn_fn)
    assert_nonzero(preads4falcon_fasta_fn)
    support.run_falcon_asm(**args)
    run_cmd('bash %s' %script_fn, sys.stdout, sys.stderr, shell=False)
    p_ctg = 'p_ctg.fa'
    assert_nonzero(p_ctg)
    n_records = _linewrap_fasta(p_ctg, o_fasta_fn)
    if n_records == 0:
        # We already checked 0-length, but maybe this is still possible.
        # Really, we want to detect 0 base-length, but I do not know how yet.
        raise Exception("No records found in primary contigs: '%s'" %os.path.abspath(p_ctg))
    say('Finished run_falcon_asm(%s, %s)' %(repr(input_files), repr(output_files)))
    return 0

def run_hgap(input_files, output_files):
    i_cfg_fn, i_logging_fn, i_subreadset_fn = input_files
    o_fasta_fn, = output_files
    # Update the cfg with our subreadset.
    # Run pypeflow.hgap.main.
    cmd = 'python -m pbfalcon.cli.hgap_run --logging {} {}'.format(i_logging_fn, i_cfg_fn)
    #cmd = 'python -m pbfalcon.cli.hgap_run {}'.format(i_cfg_fn)
    system(cmd)
    final_asm_fn = os.path.join('2-asm-falcon', 'p_ctg.fa') # TODO: Polish!
    cmd = 'mkdir -p %s; touch %s' %(os.path.dirname(final_asm_fn), final_asm_fn)
    system(cmd)
    # Link the output fasta to the final assembly of HGAP.
    symlink(final_asm_fn, o_fasta_fn)
    return 0

def run_report_preassembly_yield(input_files, output_files):
    i_json_config_fn, i_preads_fofn_fn, i_raw_reads_fofn_fn = input_files
    o_json_fn, = output_files
    kwds = {
        'i_json_config_fn': i_json_config_fn,
        'i_raw_reads_fofn_fn': i_raw_reads_fofn_fn,
        'i_preads_fofn_fn': i_preads_fofn_fn,
        'o_json_fn': o_json_fn,
    }
    report_preassembly.for_task(**kwds)
    return 0

def assert_nonzero(fn):
    if filesize(fn) == 0:
        raise Exception("0-length filesize for: '%s'" %os.path.abspath(fn))
