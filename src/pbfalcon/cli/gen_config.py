from pbcommand.cli import pbparser_runner
from pbcommand.utils import setup_log
from pbcommand.models import (ResourceTypes, FileTypes)
from pbcommand.models.parser import get_pbparser
from .. import tusks
import sys
import logging

__version__ = '1.0.0'
log = logging.getLogger(__name__)
TOOL_ID = 'pbfalcon.tasks.task_falcon_gen_config'

# temporary defaults for lambda
# see: http://bugzilla.nanofluidics.com/show_bug.cgi?id=28896
_defaults_for_task_falcon_get_config = """\
falcon_sense_option = --output_multi --min_idt 0.70 --min_cov 1 --local_match_count_threshold 100 --max_n_read 20000 --n_core 6
length_cutoff = 1
length_cutoff_pr = 1
pa_DBsplit_option = -x5 -s50 -a
pa_HPCdaligner_option =  -v -k25 -h35 -w5 -H1000 -e.95 -l40 -s1000 -t27
pa_concurrent_jobs = 32
overlap_filtering_setting = --max_diff 10000 --max_cov 100000 --min_cov 0 --bestn 1000 --n_core 4
ovlp_HPCdaligner_option =  -v -k25 -h35 -w5 -H1000 -e.99 -l40 -s1000 -t27
ovlp_DBsplit_option = -x5 -s50 -a
ovlp_concurrent_jobs = 32
"""
# also see:
#   https://dazzlerblog.wordpress.com/command-guides/daligner-command-reference-guide/
#   https://dazzlerblog.wordpress.com/2014/06/01/the-dazzler-db/
#   https://github.com/PacificBiosciences/FALCON/wiki/Manual

def sorted_str(s):
    return '\n'.join(sorted(s.splitlines()))

_defaults_for_task_falcon_get_config = sorted_str(_defaults_for_task_falcon_get_config)


def add_args_and_options(p):
    # FileType, label, name, description
    p.add_input_file_type(FileTypes.FOFN, "fofn_in", "FileOfFileNames", "FOFN for fasta files")
    # File Type, label, name, description, default file name
    p.add_output_file_type(FileTypes.TXT, "cfg_out", "INI File", "FALCON cfg (aka 'ini')", 'fc_run.cfg')
    # Option id, label, default value, name, description
    p.add_str("falcon_ns.task_options.GenomeSize_int", "genome-size", '5000000',
            "Genome size (base pairs)", "Approx. number of base pairs expected in the genome.")
    p.add_str("falcon_ns.task_options.ParallelTasksMax_int", "parallel-tasks-max", '10',
            "Parallel Tasks Max.", "Maximum number of simultaneous tasks. Currently, this will set an upper bound on the number of 'chunks'.")
    p.add_str("falcon_ns.task_options.ParallelProcsMax_int", "parallel-procs-max", '40',
            "Parallel Procs Max.", "Maximum number of processors in use across the network. Note that a Task can use multiple processors.")
    falcon_semi_default = tusks.ini2option_text(_defaults_for_task_falcon_get_config)
    p.add_str("falcon_ns.task_options.FalconCfg_str", "falcon-cfg", falcon_semi_default,
            "FALCON cfg overrides", "This is intended to allow support engineers to overrides the config which we will generate from other options. It is a semicolon-separated list of key=val pairs. Newlines are allowed by ignored. For more details on the available options, see https://github.com/PacificBiosciences/FALCON/wiki/Manual")
    return p

def get_contract_parser():
    # Number of processors to use, can also be SymbolTypes.MAX_NPROC
    nproc = 1
    # Log file, tmp dir, tmp file. See ResourceTypes in models, ResourceTypes.TMP_DIR
    resource_types = ()
    # Commandline exe to call "{exe}" /path/to/resolved-tool-contract.json
    driver_exe = "python -m pbfalcon.cli.gen_config --resolved-tool-contract "
    desc = "Generate FALCON cfg from pbcommand options."
    name = 'Tool FalconConfigGenerator'
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
