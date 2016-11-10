#!/usr/bin/env python
"""
Specialized scatter task which spawns an input pickle file
containing scripts into at most max_nchunk scattered pickles.

Input pickle file is dict{p_id: args}, where args is
dict{'script_fn': script_fn, 'script_dir': script_dir}.

Each scattered pickle file has the same data type, but likely
contains less items.
"""

import logging
import os.path as op
import sys
import cPickle

from pbcommand.utils import setup_log
from pbcommand.cli import pbparser_runner
from pbcommand.models import get_scatter_pbparser, FileTypes, PipelineChunk
from pbcommand.pb_io import write_pipeline_chunks


log = logging.getLogger(__name__)


class Constants(object):
    """Constans used in pbfalcon.tasks.scatter_run_scripts_in_pickle"""
    TOOL_ID = "pbfalcon.tasks.scatter_run_scripts_in_pickle"
    DEFAULT_NCHUNKS = 24
    VERSION = "0.1.0"
    DRIVER_EXE = "python -m %s --resolved-tool-contract " % TOOL_ID
    CHUNK_KEYS = ['$chunk.pickle_id']


def get_contract_parser():
    """
    input:
      idx 0: pickle_id
    output:
      idx 0: chunk json
    """
    p = get_scatter_pbparser(Constants.TOOL_ID, Constants.VERSION,
                             "Scatter Pickle with Scripts into Chunks",
                             __doc__, Constants.DRIVER_EXE,
                             chunk_keys=Constants.CHUNK_KEYS,
                             is_distributed=True)

    p.add_input_file_type(FileTypes.PICKLE, "pickle_with_scripts",
                          "PICKLE", "Pickle containing scripts") # input idx 0
    p.add_output_file_type(FileTypes.CHUNK, "cjson_out",
                           "Chunk JSON", "Chunked JSON",
                           "pickle_with_scripts.chunked")
    # max nchunks for this specific task
    p.add_int("pbsmrtpipe.task_options.dev_scatter_max_nchunks", "max_nchunks",
              Constants.DEFAULT_NCHUNKS,
              "Max NChunks", "Maximum number of Chunks")
    return p


def run_main(pickle_file, output_json_file, max_nchunks):
    """
    Spawn a pickle with scripts into multiple pickles each containing a script.
    Parameters:
      pickle_file -- pickle <- dict{p_id: args}, where args <- dict{'script_fn': script_fn, ...}
      output_json -- chunk.json
    """
    a = cPickle.load(open(pickle_file, 'rb'))
    if len(a) == 0:
        raise ValueError("script pickle %s is empty" % pickle_file)
    out_dir = op.dirname(output_json_file)

    num_chunks = min(max_nchunks, len(a))
    num_scripts_per_chunk = int(len(a)/num_chunks)

    # Writing chunk.json
    base_name = "spawned_pickle_w_scripts_chunk"
    chunks = []
    spawned_pickles = []

    p_ids = sorted(a.keys())
    for chunk_idx in range(0, num_chunks):
        chunk_id = "_".join([base_name, str(chunk_idx)])
        spawned_pickle_file = op.join(out_dir, chunk_id + ".pickle")
        # make a chunk
        print Constants.CHUNK_KEYS[0]
        d = {Constants.CHUNK_KEYS[0]: spawned_pickle_file}
        c = PipelineChunk(chunk_id, **d)
        chunks.append(c)

        # make content for the spawned pickle
        scripts_dict = dict()
        num_scripts = min(num_scripts_per_chunk, len(p_ids))
        for script_idx in range(0, num_scripts):
            p_id = p_ids[script_idx]
            scripts_dict[p_id] = a[p_id]

        # delete p_ids[0: num_scripts]
        p_ids = p_ids[num_scripts:]

        # Write script_dict, which is a dict of {p_id: args} to spawned pickle
        cPickle.dump(scripts_dict, open(spawned_pickle_file, 'wb'))
        spawned_pickles.append(spawned_pickle_file)

    log.info("Spawning %s into %d files", pickle_file, num_chunks)
    log.debug("Spawned files: %s.", ", ".join(spawned_pickles))
    log.info("Writing chunk.json to %s", output_json_file)
    write_pipeline_chunks(chunks, output_json_file,
                          "created by %s" % Constants.TOOL_ID)
    return 0


def args_run(dummy_args):
    """Args runner."""
    raise NotImplementedError()


def rtc_runner(rtc):
    """Resolved tool contract runner."""
    return run_main(pickle_file=rtc.task.input_files[0],
                    output_json_file=rtc.task.output_files[0],
                    max_nchunks=rtc.task.max_nchunks)


def main():
    """Main"""
    mp = get_contract_parser()
    return pbparser_runner(sys.argv[1:],
                           mp,
                           args_run,
                           rtc_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
