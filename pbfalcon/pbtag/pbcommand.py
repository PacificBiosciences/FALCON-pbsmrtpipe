"""Copied verbatim from pbcommand.
- pbcommand/models/common.py
- pbcommand/pb_io/common.py
"""
import json
import logging
import re
import sys

log = logging.getLogger(__name__)


def _is_chunk_key(k):
    return k.startswith(PipelineChunk.CHUNK_KEY_PREFIX)


class MalformedChunkKeyError(ValueError):

    """Chunk Key does NOT adhere to the spec"""
    pass


class PipelineChunk(object):

    CHUNK_KEY_PREFIX = "$chunk."
    RX_CHUNK_KEY = re.compile(r'^\$chunk\.([A-z0-9_]*)')

    def __init__(self, chunk_id, **kwargs):
        """

        kwargs is a key-value store. keys that begin "$chunk." are considered
        to be semantically understood by workflow and can be "routed" to
        chunked task inputs.

        Values that don't begin with "$chunk." are considered metadata.


        :param chunk_id: Chunk id
        :type chunk_id: str

        """
        if self.RX_CHUNK_KEY.match(chunk_id) is not None:
            raise MalformedChunkKeyError("'{c}' expected {p}".format(c=chunk_id, p=self.RX_CHUNK_KEY.pattern))

        self.chunk_id = chunk_id
        # loose key-value pair
        self._datum = kwargs

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.chunk_id, c=",".join(self.chunk_keys))
        return "<{k} id='{i}' chunk keys={c} >".format(**_d)

    def set_chunk_key(self, chunk_key, value):
        """Overwrite or add a chunk_key => value to the Chunk datum

        the chunk-key can be provided with or without the '$chunk:' prefix
        """
        if not chunk_key.startswith(PipelineChunk.CHUNK_KEY_PREFIX):
            chunk_key = PipelineChunk.CHUNK_KEY_PREFIX + chunk_key
        self._datum[chunk_key] = value

    def set_metadata_key(self, metadata_key, value):
        """Set chunk metadata key => value

        metadata key must NOT begin with $chunk. format
        """
        if metadata_key.startswith(PipelineChunk.CHUNK_KEY_PREFIX):
            raise ValueError("Cannot set chunk-key values. {i}".format(i=metadata_key))
        self._datum[metadata_key] = value

    @property
    def chunk_d(self):
        return {k: v for k, v in self._datum.iteritems() if _is_chunk_key(k)}

    @property
    def chunk_keys(self):
        return self.chunk_d.keys()

    @property
    def chunk_metadata(self):
        return {k: v for k, v in self._datum.iteritems() if not _is_chunk_key(k)}

    def to_dict(self):
        return {'chunk_id': self.chunk_id, 'chunk': self._datum}


def write_pipeline_chunks(chunks, output_json_file, comment):

    _d = dict(nchunks=len(chunks), _version="0.1.0",
              chunks=[c.to_dict() for c in chunks])

    if comment is not None:
        _d['_comment'] = comment

    with open(output_json_file, 'w') as f:
        f.write(json.dumps(_d, indent=4, separators=(',', ': ')))

    log.debug("Write {n} chunks to {o}".format(n=len(chunks), o=output_json_file))


def load_pipeline_chunks_from_json(path):
    """Returns a list of Pipeline Chunks


    :rtype: list[PipelineChunk]
    """

    try:
        with open(path, 'r') as f:
            d = json.loads(f.read())

        chunks = []
        for cs in d['chunks']:
            chunk_id = cs['chunk_id']
            chunk_datum = cs['chunk']
            c = PipelineChunk(chunk_id, **chunk_datum)
            chunks.append(c)
        return chunks
    except Exception:
        msg = "Unable to load pipeline chunks from {f}".format(f=path)
        sys.stderr.write(msg + "\n")
        raise
