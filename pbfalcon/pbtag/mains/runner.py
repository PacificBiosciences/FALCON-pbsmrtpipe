#!/usr/bin/env python2.7
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
import argparse
import json
import logging
import os
import pprint
import sys
import time

LOG = logging.getLogger()
LOG.setLevel(logging.DEBUG) # For now, let Handlers control the levels.

def system(call, checked=False):
    LOG.info(call)
    rc = os.system(call)
    msg = '{} <- {!r}'.format(rc, call)
    LOG.debug(msg)
    if checked and rc:
        raise Exception(msg)
    return rc

def touch(fname):
    with open(fname, 'a'):
        os.utime(fname, None)

def foo(srtc):
    LOG.info('In foo')
    outputs = srtc['output_files']
    options = srtc['options']
    import pprint
    print('options:{}'.format(pprint.pformat(options)))
    uows = options['snafu.task_options.uows']
    with open(outputs[0], 'w') as stream:
        data = ['FOO{}'.format(i) for i in range(uows)]
        json_txt = json.dumps(data, indent=2)
        stream.write(json_txt)
def bar(srtc):
    LOG.info('In bar')
    outputs = srtc['output_files']
    touch(outputs[0])
def fubar(srtc):
    LOG.info('In fubar')
    inputs = srtc['input_files']
    outputs = srtc['output_files']
    with open(inputs[0]) as stream:
        data = json.loads(stream.read())
    with open(outputs[0], 'w') as stream:
        stream.write(json.dumps(data))
def scatter_fubar(srtc):
    LOG.info('In scatter_fubar')
    inputs = srtc['input_files']
    outputs = srtc['output_files']
    max_nchunks = srtc['max_nchunks']
    #chunk_keys = srtc['chunk_keys']
    from . import scatter_json_list
    scatter_json_list.run('scatter_fubar', max_nchunks, inputs[0], outputs[0])
def scatter_json_list_plus_txt(srtc):
    LOG.info('In scatter_json_list_plus_txt: {}'.format(repr(srtc)))
    inputs = srtc['input_files']
    outputs = srtc['output_files']
    max_nchunks = srtc['max_nchunks']
    tcid = srtc['tool_contract_id']
    basename = os.path.splitext(tcid)[1][1:]
    #chunk_keys = srtc['chunk_keys']
    from . import scatter_json_list_plus_txt
    scatter_json_list_plus_txt.run(basename, max_nchunks, inputs[0], inputs[1], outputs[0])
def gather_json_list(srtc):
    LOG.info('In gather_json')
    inputs = srtc['input_files']
    outputs = srtc['output_files']
    chunk_key = srtc['chunk_key']
    chunk_input_json_fn = inputs[0]
    output_fn = outputs[0]
    from . import gather_json_list
    gather_json_list.run(chunk_key, chunk_input_json_fn, output_fn)
def run_rtc(args):
    setup_logging(args)

    LOG.info('sys.executable={!r}'.format(sys.executable))
    LOG.info('Parsed args (after logging setup): {!r}'.format(vars(args)))
    LOG.info('rtc_path: {!r}'.format(args.rtc_path))
    rtc_path = args.rtc_path

    rtc = json.load(open(args.rtc_path))
    LOG.info('rtc: {!s}'.format(pprint.pformat(rtc)))
    srtc = rtc['resolved_tool_contract']
    tcid = srtc['tool_contract_id']
    options = srtc['options']
    log_level = srtc['log_level']
    input_files = srtc['input_files']
    output_files = srtc['output_files']
    nproc = srtc['nproc']
    #resources = srtc['resources']

    task_func = {
            'foo': foo,
            'bar': bar,
            'task_run_fubar_jobs': fubar,
            'scatter_fubar': scatter_fubar,
            'gather_fubar': gather_json_list,
            'task_falcon0_run_daligner_jobs_scatter': scatter_json_list_plus_txt,
            'task_falcon0_run_las_merge_jobs_scatter': scatter_json_list_plus_txt,
            'task_falcon0_run_cns_jobs_scatter': scatter_json_list_plus_txt,
            'task_falcon1_run_daligner_jobs_scatter': scatter_json_list_plus_txt,
            'task_falcon1_run_las_merge_jobs_scatter': scatter_json_list_plus_txt,

            'task_falcon0_run_daligner_jobs_gather': gather_json_list,
            'task_falcon0_run_las_merge_jobs_gather': gather_json_list,
            'task_falcon0_run_cns_jobs_gather': gather_json_list,
            'task_falcon1_run_daligner_jobs_gather': gather_json_list,
            'task_falcon1_run_las_merge_jobs_gather': gather_json_list,
    }
    func_name = os.path.splitext(tcid)[1][1:]
    func = task_func[func_name]

    func(srtc)

def emit_one(args):
    pass
def emit_all(args):
    pass

def setup_logging(args):
    handler = get_logging_handler(args)
    LOG.addHandler(handler)
    try:
        import logging_tree
        print('logging_tree:')
        logging_tree.printout()
    except ImportError:
        pass

    del_logging_flags(args)

def get_logging_handler(args):
    """Return new logging Handler.
    Also, remove related flags from argparse args.
    """
    fmt = '[%(levelname)s]%(message)s'
    log_level = args.log_level
    if args.log_level is not None:
        log_level = args.log_level
    if args.verbose:
        log_level = 'INFO'
    if args.quiet:
        log_level = 'CRITICAL'
    if args.debug:
        log_level = 'DEBUG'
        fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt=fmt)
    logging.Formatter.converter = time.gmtime
    if args.log_file:
        handler = logging.FileHandler(args._log_file, mode='a')
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.setLevel(log_level)
    return handler

def add_logging_flags(parser):
    """
  --log-file LOG_FILE   Write the log to file. Default(None) will write to
                        stdout.
  --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set log level (default: INFO)
    """
    parser.add_argument('--log-file',
            help='Write the log to file. By default, write to stdout.')
    parser.add_argument('--log-level',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO',
            help='Set log level.')
    parser.add_argument('--verbose', '-v',
            help='Set the verbosity level. (Only partially supported for now.)')
    parser.add_argument('--quiet',
            help='Alias for setting log level to CRITICAL')
    parser.add_argument('--debug',
            help='Alias for setting log level to DEBUG')

def del_logging_flags(args):
    delattr(args, 'log_file')
    delattr(args, 'log_level')
    delattr(args, 'verbose')
    delattr(args, 'quiet')
    delattr(args, 'debug')


class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass

def main(argv=sys.argv):
    description = 'Multi-quick-tool-runner for pbsmrtpipe tasks'
    epilog = 'Real tool should be inferred from the resolved_tool_contract->tool_contract_id field.'
    parser = argparse.ArgumentParser(
            description=description, epilog=epilog,
            formatter_class=HelpF,
    )
    parser.add_argument('--version',
            action='version', version='0.0.0',
            help="show program's version number and exit"
    )

    subparsers = parser.add_subparsers(
            help='sub-command help')
    parser_run = subparsers.add_parser('run-rtc',
            formatter_class=HelpF)
    parser_emit_one = subparsers.add_parser('emit-tool-contract',
            formatter_class=HelpF)
    parser_emit_all = subparsers.add_parser('emit-tool-contracts',
            formatter_class=HelpF)
    parser_run.set_defaults(func=run_rtc)
    parser_emit_one.set_defaults(func=emit_one)
    parser_emit_all.set_defaults(func=emit_all)

    parser_run.add_argument('rtc_path',
            help='Path to resolved tool contract')

    parser_emit_one.add_argument('tc_id',
            help='Tool Contract Id')

    parser_emit_all.add_argument('--output-dir', '-o',
            default=os.getcwd(),
            help='Emit all Tool Contracts to output directory')

    add_logging_flags(parser_run)

    args = parser.parse_args(argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
