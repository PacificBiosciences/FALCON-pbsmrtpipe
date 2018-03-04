from __future__ import division
from __future__ import unicode_literals
from ..pbcommand import (PipelineChunk, write_pipeline_chunks)
import json
import logging
import os

LOG = logging.getLogger(__name__)
CHUNK_KEYS = ['$chunk.scatterkeyjsonlist', '$chunk.scatterkeynoop']

def write_desc_of_chunks(filenames, desc_of_chunks_fn, chunk_id_template='mychunk_{}'):
    """Write a description of our new chunks (in JSON).
    The format is specified by pbcommand.
    Each filename-tuple gets a chunk_id.
    """
    def chunk():
        for i, fn_tuple in enumerate(filenames):
            chunk_id = chunk_id_template.format(i)
            d = dict()
            for i in range(len(CHUNK_KEYS)):
                d[CHUNK_KEYS[i]] = os.path.abspath(fn_tuple[i])
            c = PipelineChunk(chunk_id, **d)
            yield c
    chunks = list(chunk())
    comment = 'created by ' + __name__
    write_pipeline_chunks(chunks, desc_of_chunks_fn, comment=comment)

def num_items_in_each_chunk(num_items, num_chunks):
    """
    Put num_items items to num_chunks drawers as even as possible.
    Return a list of integers, where ret[i] is the number of items put to drawer i.
    """
    if num_chunks == 0:
        return [0]
    num_items_per_chunk = num_items // num_chunks
    ret = [num_items_per_chunk] * num_chunks
    for i in range(0, num_items % num_chunks):
        ret[i] = ret[i] + 1
    assert sum(ret) == num_items
    return ret

def run(task_basename, max_nchunks, input_data_fn, input_txt_fn, chunk_output_json_fn):
    # Given input with a set of N data, write NC outputs ("chunks") with roughly
    # N/NC sets of data each, and write a JSON file to describe them.
    # Also copy the txt, but do not alter it.
    # NC must be <= max_nchunks.
    # Then, write a chunk-description file (JSON).
    def yield_each_datum():
        with open(input_data_fn) as stream:
            data = json.loads(stream.read())
            for datum in data:
                yield datum
    nd = sum(1 for _ in yield_each_datum()) # count data
    nchunks = min(max_nchunks, nd)
    list_of_nsd = num_items_in_each_chunk(num_items=nd, num_chunks=nchunks)
    all_data = yield_each_datum()
    filename_template = task_basename + '_chunk{}.data.json'
    def yield_names_of_new_files():
        """And write each new file.
        """
        for i, nsd in enumerate(list_of_nsd):
            filename = filename_template.format(i)
            with open(filename, 'w') as stream:
                these_data = list()
                for j in range(nsd):
                    datum = next(all_data)
                    these_data.append(datum)
                stream.write(json.dumps(these_data, indent=4))
            yield (filename, input_txt_fn) # Note: input_txt is same everywhere.
    chunk_id_template = task_basename + '_{}'
    write_desc_of_chunks(yield_names_of_new_files(), chunk_output_json_fn, chunk_id_template)
