from pbcommand.cli import pbparser_runner
from pbcommand.utils import setup_log
from pbcommand.models import (ResourceTypes, FileTypes, SymbolTypes)
from pbcommand.models.parser import get_pbparser
from .. import hgap_prepare
import sys
import logging

__version__ = '1.0.0'
log = logging.getLogger(__name__)
TOOL_ID = 'falcon_ns.tasks.task_hgap_prepare'

default_pbalign_section = """
    "pbalign": {
        "options": "--hitPolicy randombest --minAccuracy 70.0 --minLength 50 --algorithm=blasr --concordant",
        "algorithmOptions": "-minMatch 12 -bestn 10 -minPctSimilarity 70.0",
        "_jdnotes": "--maxHits 1 --minAnchorSize 12 --maxDivergence=30 --minAccuracy=0.75 --minLength=50 --hitPolicy=random --seed=1",
        "_comment": "Overrides for blasr alignment (prior to polishing)"
    },
"""
default_variantCaller_section = """
    "variantCaller": {
        "options": "--algorithm quiver --diploid --min_confidence 40 --min_coverage 5"
        "_comment": "Overrides for genomic consensus (polishing)"
    },
"""
# See "Minimal Options" at:
#   https://github.com/PacificBiosciences/ExperimentalPipelineOptionsDocs/blob/master/HGAP/defaults.md
default_HGAP_Options = ""

def add_args_and_options(p):
    # FileType, label, name, description
    p.add_input_file_type(FileTypes.DS_SUBREADS, "subreads-in", "DataSet-SubreadSet", "Input: Probably BAM files")
    # File Type, label, name, description, default file name
    p.add_output_file_type(FileTypes.JSON, "hgap-cfg-out", "HGAP JSON file", "Output: Actual configuration to be used by HGAP, in a 2-level dictionary.", 'hgap-cfg.json')
    p.add_output_file_type(FileTypes.JSON, "logging-cfg-out", "Python logging.config JSON file", "Output: Standard Python logging.config (for the task, not pbsmrtpipe)", 'logging-cfg.json')
    # Option id, label, default value, name, description
    p.add_str("falcon_ns.task_options." + hgap_prepare.OPTION_CFG, "advanced-overrides",
            default_HGAP_Options,
            "Experimental HGAP.5 config overrides.",
            "Experimental HGAP.5 config overrides are experimental.")
    return p

def get_contract_parser():
    # Number of processors to use, can also be SymbolTypes.MAX_NPROC
    nproc = SymbolTypes.MAX_NPROC
    # Log file, tmp dir, tmp file. See ResourceTypes in models, ResourceTypes.TMP_DIR
    resource_types = ()
    # Commandline exe to call "{exe}" /path/to/resolved-tool-contract.json
    driver_exe = "python -m pbfalcon.cli.hgap_prepare --resolved-tool-contract "
    desc = "XXX Experimental HGAP.5"
    name = 'XXX Experimental HgapConfigGenerator'
    p = get_pbparser(TOOL_ID, __version__, name, desc, driver_exe,
            is_distributed=True, nproc=nproc, resource_types=resource_types)
    add_args_and_options(p)
    return p

def run_my_main(input_files, output_files, options):
    # do stuff. Main should return an int exit code
    rc = hgap_prepare.run_hgap_prepare(input_files, output_files, options)
    if rc:
        return rc
    else:
        return 0

def _args_runner(args):
    # this is the args from parser.parse_args()
    # the properties of args are defined as "labels" in the add_args_and_options func.
    # TODO: Convert 'args' to a dict somehow?
    return run_my_main([args.fasta_in], [args.fasta_out], args)

def _resolved_tool_contract_runner(resolved_tool_contract):
    rtc = resolved_tool_contract
    # all options are referenced by globally namespaced id. This allows tools to use other tools options
    # e.g., pbalign to use blasr defined options.
    return run_my_main(rtc.task.input_files, rtc.task.output_files, rtc.task.options)

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