# This was in pbsmrtpipe/tools/chunk_utils.py
from pbcommand.models import PipelineChunk
from pbsmrtpipe.tools.chunk_utils import write_chunks_to_json
import os


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
