"""This file is an experimental WIP.
"""
from falcon_kit import run_support as support
import pbcommand.cli
import pbsmrtpipe.cli_utils
import pbsmrtpipe.utils

import logging
import sys

__version__ = '1.0.0'
log = logging.getLogger()

def _add_an_option(p):
    p.add_argument('config',
        type=pbsmrtpipe.cli_utils.validate_file,
        help="Specify path to config file.")
    return p
def _get_parser():
    desc = "Runner for FALCON with pbsmrtpipe jobs."
    p = pbcommand.cli.get_default_argparser(__version__, desc)
    funcs = [
        _add_an_option
    ]
    f = pbsmrtpipe.utils.compose(*funcs)
    p = f(p)
    return p

def _run_from_args(args):
    print("_run_from_args(%s)" %repr(args))
    log.info( "pb-falcon started with configuration %s", args.config ) 
    config = support.get_config(support.parse_config(args.config))
    return 0

def main(argv=sys.argv):
    print(argv)
    parser = _get_parser()
    return pbcommand.cli.pacbio_args_runner(
            argv[1:], parser, _run_from_args,
            log, pbsmrtpipe.utils.setup_log)
