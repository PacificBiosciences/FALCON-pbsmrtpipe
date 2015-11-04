from pbcommand.cli import pbparser_runner
from pbcommand.utils import setup_log
from pbcommand.models import (ResourceTypes, FileTypes)
from pbcommand.models.parser import get_pbparser
import sys
import logging

__version__ = '1.0.0'
log = logging.getLogger(__name__)
TOOL_ID = 'pbfalcon_id.tasks.foo'


def add_args_and_options(p):
    # FileType, label, name, description
    p.add_input_file_type(FileTypes.FASTA, "fasta_in", "Fasta File", "PacBio Spec'ed fasta file")
    # File Type, label, name, description, default file name
    p.add_output_file_type(FileTypes.FASTA, "fasta_out", "Filtered Fasta file", "Filtered Fasta file", "filter.fasta")
    # Option id, label, default value, name, description
    # for the argparse, the read-length will be translated to --read-length and (accessible via args.read_length)
    p.add_int("pbcommand.task_options.dev_read_length", "read-length", 25, "Length filter", "Min Sequence Length filter")
    return p

def get_contract_parser():
    # Number of processors to use, can also be SymbolTypes.MAX_NPROC
    nproc = 1
    # Log file, tmp dir, tmp file. See ResourceTypes in models, ResourceTypes.TMP_DIR
    resource_types = ()
    # Commandline exe to call "{exe}" /path/to/resolved-tool-contract.json
    driver_exe = "python -m pbfalcon.cli.config --resolved-tool-contract "
    desc = "Generate FALCON cfg from pbcommand options."
    name = 'FooName'
    p = get_pbparser(TOOL_ID, __version__, name, desc, driver_exe,
            is_distributed=False, nproc=nproc, resource_types=resource_types)
    add_args_and_options(p)
    return p

def run_my_main(fasta_in, fasta_out, min_length):
    # do stuff. Main should return an int exit code
    return 0

def _args_runner(args):
    # this is the args from parser.parse_args()
    # the properties of args are defined as "labels" in the add_args_and_options func.
    return run_my_main(args.fasta_in, fasta_out, args.read_length)

def _resolved_tool_contract_runner(resolved_tool_contract):
    rtc = resolved_tool_contract
    # all options are referenced by globally namespaced id. This allows tools to use other tools options
    # e.g., pbalign to use blasr defined options.
    return run_my_main(rtc.inputs[0], rtc.outputs[0], rtc.options["pbcommand.task_options.dev_read_length"])

def main(argv=sys.argv):
    log.info("Starting {f} version {v} pbcommand example dev app".format(f=__file__, v=__version__))
    p = get_contract_parser()
    return pbparser_runner(argv[1:],
                           p,
                           _args_runner, # argparse runner func
                           _resolved_tool_contract_runner, # tool contract runner func
                           log, # log instance
                           setup_log # setup log func
                           )
if __name__ == '__main__':
    sys.exit(main())
