from __future__ import absolute_import
from sys import stderr, stdout
from contextlib import contextmanager
import logging
import os

LOG = logging.getLogger(__name__)

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    say('CD: %r <- %r' %(newdir, prevdir))
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        say('CD: %r -> %r' %(newdir, prevdir))
        os.chdir(prevdir)

def lg(msg):
    #print(msg)
    LOG.info(msg)

say=lg

def symlink(actual, symbolic=None, force=True):
    """Symlink into cwd, relatively.
    symbolic name is basename(actual) if not provided.
    If not force, raise when already exists and does not match.
    But ignore symlink to self.
    (copied from falcon_kit.mains.dazzler.py)
    """
    symbolic = os.path.basename(actual) if not symbolic else symbolic
    if os.path.abspath(actual) == os.path.abspath(symbolic):
        LOG.warning('Cannot symlink {!r} as {!r}, itself.'.format(actual, symbolic))
        return
    rel = os.path.relpath(actual)
    if force:
        LOG.info('ln -sf {} {}'.format(rel, symbolic))
        if os.path.lexists(symbolic):
            if os.readlink(symbolic) == rel:
                return
            else:
                os.unlink(symbolic)
    else:
        LOG.info('ln -s {} {}'.format(rel, symbolic))
        if os.path.lexists(symbolic):
            if os.readlink(symbolic) != rel:
                msg = '{!r} already exists as {!r}, not {!r}'.format(
                        symbolic, os.readlink(symbolic), rel)
                raise Exception(msg)
            else:
                LOG.info('{!r} already points to {!r}'.format(symbolic, rel))
                return
    os.symlink(rel, symbolic)

def unlink(*fns):
    for fn in fns:
        if os.path.lexists(fn):
            #lg('rm -f {}'.format(fn))
            os.unlink(fn)

def system(cmd):
    lg('system(%s)' %repr(cmd))
    rc = os.system(cmd)
    if rc:
        raise Exception('%d <- %r' %(rc, cmd))

def filesize(path):
    statinfo = os.stat(path)
    return statinfo.st_size
