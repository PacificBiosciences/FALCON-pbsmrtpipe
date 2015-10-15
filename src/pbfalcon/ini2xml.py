#!/usr/bin/env python2.7
"""Convert our cfg file to preset.xml section.

This is not part of the workflow. It is only a utility, used as needed.

https://github.com/PacificBiosciences/FALCON-pbsmrtpipe/issues/2
"""
import ConfigParser
import pprint
import sys

def xml(write, tag, **attrs):
    extra = ''.join(' {}={}'.format(k, v) for k,v in attrs)
    write('<{}{}>'.format(tag, extra))
def parse_config(ifp):
    config = ConfigParser.ConfigParser()
    config.readfp(ifp)
    return config
def get_dict(cfg, sec='General'):
    return dict((key, val) for key, val in cfg.items(sec))
def dump(ofp, data):
    ofp.write(pprint.pformat(data))
def convert(ifp, ofp):
    config = parse_config(ifp)
    data = get_dict(config)
    dump(ofp, data)
def main():
    ifs = sys.stdin
    ofs = sys.stdout
    convert(ifs, ofs)

if __name__=="__main__":
    main(*sys.argv[1:])
