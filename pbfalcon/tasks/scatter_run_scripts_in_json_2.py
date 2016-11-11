#!/usr/bin/env python
"""
Specialized scatter task which spawns an input json file
containing scripts into at most max_nchunk scattered json files.

Input json file is dict{p_id: dict{'script_fn': script_fn, 'script_dir': script_dir}}.

Input txt file is a sentinel file.

Each scattered json file has the same data type, but likely
contains less items.
"""

import logging
import os.path as op
import sys
import json

from pbcommand.utils import setup_log
from pbcommand.cli import pbparser_runner
from pbcommand.models import get_scatter_pbparser, FileTypes, PipelineChunk
from pbcommand.pb_io import write_pipeline_chunks


log = logging.getLogger(__name__)


class Constants(object):
    """Constans used in pbfalcon.tasks.scatter_run_scripts_in_json_2"""
    TOOL_ID = "pbfalcon.tasks.scatter_run_scripts_in_json_2"
    DEFAULT_NCHUNKS = 24
    VERSION = "0.1.0"
    DRIVER_EXE = "python -m %s --resolved-tool-contract " % TOOL_ID
    CHUNK_KEYS = ['$chunk.json_id', '$chunk.txt_id']


def get_contract_parser():
    """
    input:
      idx 0: json_id
      idx 1: txt_id
    output:
      idx 0: chunk json
    """
    p = get_scatter_pbparser(Constants.TOOL_ID, Constants.VERSION,
                             "Scatter json with Scripts into Chunks",
                             __doc__, Constants.DRIVER_EXE,
                             chunk_keys=Constants.CHUNK_KEYS,
                             is_distributed=True)

    p.add_input_file_type(FileTypes.JSON, "json_with_scripts",
                          "JSON", "Json containing scripts") # input idx 0
    p.add_input_file_type(FileTypes.TXT, "sentinel_txt",
                          "TXT", "Sentinel txt") # input idx 1
    p.add_output_file_type(FileTypes.CHUNK, "cjson_out",
                           "Chunk JSON", "Chunked JSON",
                           "json_with_scripts.chunked")
    # max nchunks for this specific task
    p.add_int("pbsmrtpipe.task_options.dev_scatter_max_nchunks", "max_nchunks",
              Constants.DEFAULT_NCHUNKS,
              "Max NChunks", "Maximum number of Chunks")
    return p


def run_main(json_file, output_json_file, max_nchunks):
    """
    Spawn a json with scripts into multiple json files each containing a script.
    Parameters:
      json_file -- json <- dict{p_id: args}, where args <- dict{'script_fn': script_fn, ...}
      output_json -- chunk.json
    """
    a = json.load(open(json_file, 'r'))

    if len(a) == 0:
        raise ValueError("script json %s is empty" % json_file)
    out_dir = op.dirname(output_json_file)

    num_chunks = min(max_nchunks, len(a))
    num_scripts_per_chunk = int(len(a)/num_chunks)

    # Writing chunk.json
    base_name = "spawned_json_w_scripts_chunk"
    chunks = []
    spawned_jsons = []

    p_ids = sorted(a.keys())
    for chunk_idx in range(0, num_chunks):
        chunk_id = "_".join([base_name, str(chunk_idx)])
        spawned_json_file = op.join(out_dir, chunk_id + ".json")
        spawned_txt_file = op.join(out_dir, chunk_id + "_done.txt")
        # make a chunk
        print Constants.CHUNK_KEYS[0]
        d = {Constants.CHUNK_KEYS[0]: spawned_json_file,
             Constants.CHUNK_KEYS[1]: spawned_txt_file}
        c = PipelineChunk(chunk_id, **d)
        chunks.append(c)

        # make content for the spawned json
        scripts_dict = dict()
        num_scripts = min(num_scripts_per_chunk, len(p_ids))
        for script_idx in range(0, num_scripts):
            p_id = p_ids[script_idx]
            scripts_dict[p_id] = a[p_id]

        # delete p_ids[0: num_scripts]
        p_ids = p_ids[num_scripts:]

        # Write script_dict, which is a dict of {p_id: args} to spawned json
        with open(spawned_json_file, 'w') as writer:
            writer.write(json.dumps(scripts_dict) + "\n")

        spawned_jsons.append(spawned_json_file)
        with open(spawned_txt_file, 'w') as writer:
            writer.write("%s" % spawned_json_file)

    log.info("Spawning %s into %d files", json_file, num_chunks)
    log.debug("Spawned files: %s.", ", ".join(spawned_jsons))
    log.info("Writing chunk.json to %s", output_json_file)
    write_pipeline_chunks(chunks, output_json_file,
                          "created by %s" % Constants.TOOL_ID)
    return 0


def args_run(dummy_args):
    """Args runner."""
    raise NotImplementedError()


def rtc_runner(rtc):
    """Resolved tool contract runner."""
    return run_main(json_file=rtc.task.input_files[0],
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
