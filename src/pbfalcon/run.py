import pbcommand.cli
import pbsmrtpipe.utils

import logging
import sys

__version__ = '1.0.0'
log = logging.getLogger()

def _run_from_args(args):
    print("_run_from_args(%s)" %repr(args))
    return 0

def _get_parser():
    desc = "Runner for FALCON with pbsmrtpipe jobs."
    p = pbcommand.cli.get_default_argparser(__version__, desc)
    funcs = [
    ]
    #f = pbsmrtpipe.utils.compose(*funcs)
    #p = f(p)
    return p

def main(argv=sys.argv):
    print(argv)
    parser = _get_parser()
    return pbcommand.cli.pacbio_args_runner(
            argv[1:], parser, _run_from_args,
            log, pbsmrtpipe.utils.setup_log)
