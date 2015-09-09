# This was in pbsmrtpipe/tools/chunk_utils.py
from pbcommand.models import PipelineChunk
from pbsmrtpipe.tools.chunk_utils import write_chunks_to_json
import logging
import os

log = logging.getLogger(__name__)

def foo(json_path, bash_path,
        max_total_nchunks, chunk_keys, dir_name,
        base_name, ext):
    for i in range(min(2, max_total_nchunks)):
        chunk_id = '_'.join([base_name, str(i)])
        chunk_name = '.'.join([chunk_id, ext])
        chunk_path = os.path.join(dir_name, chunk_name)
        open(chunk_path, 'w').write(str(i))
        d = {}
        d[chunk_keys[1]] = os.path.abspath(chunk_path)
        d[chunk_keys[0]] = json_path
        c = PipelineChunk(chunk_id, **d)
        yield c


def write_bar(chunk_file, json_path,
        bash_path,
        max_total_chunks, dir_name,
        chunk_base_name, chunk_ext, chunk_keys):
    chunks = list(foo(
        json_path,
        bash_path,
        max_total_chunks,
        chunk_keys,
        dir_name,
        chunk_base_name,
        chunk_ext))
    write_chunks_to_json(chunks, chunk_file)
    return 0

#from . import tusks

def parse_daligner_jobs(run_jobs_fn):
    """Find lines starting with 'daligner'.
    Return lists of split lines.
    """
    with open(run_jobs_fn) as f:
        for l in f :
            words = l.strip().split()
            if words[0] == 'daligner':
                yield words

def lg(msg):
    """Does log work?
    """
    print(msg)
    log.info(msg)

def symlink(actual):
    """Symlink into cwd, using basename.
    """
    symbolic = os.path.basename(actual)
    lg('ln -s %s %s' %(actual, symbolic))
    if os.path.lexists(symbolic):
        os.unlink(symbolic)
    os.symlink(actual, symbolic)

def symlink_dazzdb(actualdir, db_prefix):
    """Symlink elements of dazzler db.
    For now, 3 files.
    """
    symlink(os.path.join(actualdir, '.%s.bps'%db_prefix))
    symlink(os.path.join(actualdir, '.%s.idx'%db_prefix))
    symlink(os.path.join(actualdir, '%s.db'%db_prefix))


def write_run_daligner_chunks_falcon(
        pread_aln,
        chunk_file,
        config_json_fn,
        run_jobs_fn,
        max_total_nchunks,
        dir_name,
        chunk_base_name,
        chunk_ext,
        chunk_keys):
    if pread_aln:
        # preads
        daligner_exe = 'daligner_p'
        db_prefix = 'preads'
    else:
        # raw reads
        daligner_exe = 'daligner'
        db_prefix = 'raw_reads'
    def chunk():
        cmds = list(parse_daligner_jobs(run_jobs_fn))
        if max_total_nchunks < len(cmds):
            raise Exception("max_total_nchunks < # daligner cmds: %d < %d" %(
                max_total_nchunks, len(cmds)))
        symlink_dazzdb(os.path.dirname(run_jobs_fn), db_prefix)
        for i, words in enumerate(cmds):
            chunk_id = '_'.join([chunk_base_name, str(i)])
            chunk_name = '.'.join([chunk_id, chunk_ext])
            chunk_path = os.path.join(dir_name, chunk_name)
            cmd = ' '.join([daligner_exe] + words[1:])
            open(chunk_path, 'w').write(cmd + '\n')
            d = {}
            d[chunk_keys[1]] = os.path.abspath(chunk_path)
            d[chunk_keys[0]] = config_json_fn
            c = PipelineChunk(chunk_id, **d)
            yield c
    chunks = list(chunk())
    write_chunks_to_json(chunks, chunk_file)
