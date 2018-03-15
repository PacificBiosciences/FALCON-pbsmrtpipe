"""
From
    /home/UNIXHOME/cdunn/repo/bb/pbcoretools/pbcoretools/chunking/gather.py

TODO: Make this an executable, maybe?
"""
from __future__ import unicode_literals
from ..pbcommand import load_pipeline_chunks_from_json
import json
import logging
import os
import re

LOG = logging.getLogger(__name__)


def gather(input_files, output_file):
    """Combine lists of data into a single list of data.
    At first level of each record, substitute './' with '{filedir}/'
    in value. (We assume these are filenames relative to the directory
    of the input_file.)
    """
    data = []
    re_reldir = re.compile(r'^\./') # Find literal './'
    for input_file in input_files:
        with open(input_file) as stream:
            these_data = json.loads(stream.read())
            filedir = os.path.abspath(os.path.dirname(input_file)) + '/'
            for rec in these_data:
                for k,v in list(rec.items()):
                    mo = re_reldir.search(v)
                    if not mo:
                        continue
                    rec[k] = re_reldir.sub(filedir, v) # over-write w/ abspath
            data.extend(these_data)
    with open(output_file, "w") as stream:
        stream.write(json.dumps(data, indent=None, separators=(',', ':')))
    

def yield_data_from_chunks_by_chunk_key(chunks, chunk_key):
    LOG.info("extracting datum from chunks using chunk-key '{c}'".format(c=chunk_key))
    datum = []
    for chunk in chunks:
        if chunk_key in chunk.chunk_keys:
            value = chunk.chunk_d[chunk_key]
            yield value
        else:
            raise Exception("Unable to find chunk key '{i}' in {p}".format(i=chunk_key, p=chunk))


def run(chunk_key, chunk_input_json, output_file):
    chunks = load_pipeline_chunks_from_json(chunk_input_json)

    # Allow looseness
    if not chunk_key.startswith('$chunk.'):
        chunk_key = '$chunk.' + chunk_key
        LOG.warn("Prepending chunk key with '$chunk.' to '{c}'".format(c=chunk_key))

    chunked_files = yield_data_from_chunks_by_chunk_key(chunks, chunk_key)
    gather(chunked_files, output_file)
