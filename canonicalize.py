#!/usr/bin/env python
"""Canonicalize JSON files.

For now:

    * strip tailing whitespace
    * add final newline if missing

Note that this script is for developers only. It never needs to be installed.
"""
import glob
import os
import subprocess
import sys

def system(call):
    print(call)
    rc = os.system(call)
    if rc:
        msg = '%d <- %r' %(rc, call)
        raise Exception(msg)

def canonicalize(fn):
    fntmp = '%s.tmp' %fn
    system('git stripspace < %s > %s' %(fn, fntmp))
    os.rename(fntmp, fn)

def files(files_and_dirs):
    for file_or_dir in files_and_dirs:
        print(file_or_dir)
        if os.path.isfile(file_or_dir):
            yield file_or_dir
        else:
            print(os.path.join(file_or_dir, '*'))
            for fn in glob.glob(os.path.join(file_or_dir, '*')):
                print(fn)
                yield fn

def main(prog, *files_and_dirs):
    for fn in files(files_and_dirs):
        canonicalize(fn)

if __name__ == "__main__":
    main(*sys.argv)
