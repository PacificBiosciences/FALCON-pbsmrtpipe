"""Generate FALCON cfg (.ini file).

We plan to generate cfg in a complicated way.
But for now, we just use a look-up table,
based on ranges of the length of a genome.
"""
from falcon_kit import run_support as support
import ConfigParser as configparser
import json
import logging
import os
import pprint
import re
import StringIO


#logging.basicConfig()
log = logging.getLogger(__name__)
#log.setLevel(logging.DEBUG)

OPTION_CFG = 'HGAP_Options_JSON'
TASK_HGAP_PREPARE_CFG = 'falcon_ns.task_options.' + OPTION_CFG
DEFAULT_LOGGING_CFG = {
    'version': 1,
    'formatters': {
        'format_full': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
        'format_brief': {
            'format': '%(levelname)s: %(message)s',
        }
    },
    'filters': {
    },
    'handlers': {
        'handler_file_pypeflow': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'format_full',
            'filename': 'pypeflow.log',
            'mode': 'w',
        },
        'handler_file_pbfalcon': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'format_full',
            'filename': 'pbfalcon.log',
            'mode': 'w',
        },
        'handler_stream': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'format_brief',
            'stream': 'ext://sys.stderr',
        },
    },
    'loggers': {
        'pypeflow': {
            'level': 'NOTSET',
            'propagate': 1,
            'handlers': ['handler_file_pypeflow'],
        },
        'pbfalcon': {
            'level': 'NOTSET',
            'propagate': 1,
            'handlers': ['handler_file_pbfalcon'],
        },
    },
    'root': {
        'handlers': ['handler_stream'],
        'level': 'NOTSET',
    },
    'disable_existing_loggers': False
}
OPTION_SECTION_HGAP = 'hgap'
OPTION_SECTION_PBALIGN = 'pbalign'
OPTION_SECTION_VARIANTCALLER = 'variantcaller'
OPTION_SECTION_PBSMRTPIPE = 'pbsmrtpipe'

def get_pbsmrtpipe_opts(d):
    with open(os.path.join(d, 'resolved-tool-contract.json')) as f:
        rtc = json.loads(f.read())
    opts = rtc['resolved_tool_contract']
    assert 'nproc' in opts
    assert 'is_distributed' in opts
    assert 'resources' in opts
    # TODO: Removed any unneeded rtc opts.
    return opts

def dump_as_json(data, ofs):
    as_json = json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
    ofs.write(as_json)

def run_hgap_prepare(input_files, output_files, options):
    """Generate a config-file from options.
    """
    log.info('options to run_hgap_prepare:\n{}'.format(pprint.pformat(options)))
    i_subreadset_fn, = input_files
    o_hgap_cfg_fn, o_logging_cfg_fn = output_files
    run_dir = os.path.dirname(o_hgap_cfg_fn)

    # For now, ignore all but OPTION_CFG
    cfg_json = options[TASK_HGAP_PREPARE_CFG].strip()
    if not cfg_json:
        cfg_json = '{}'
    all_cfg = json.loads(cfg_json)
    log.info('Parsed {!r}:\n{}'.format(
        TASK_HGAP_PREPARE_CFG, all_cfg))

    # Get options from pbsmrtpipe.
    pbsmrtpipe_opts = get_pbsmrtpipe_opts(run_dir)
    if OPTION_SECTION_PBSMRTPIPE not in all_cfg:
        all_cfg[OPTION_SECTION_PBSMRTPIPE] = dict()
    pbsmrtpipe_opts.update(all_cfg[OPTION_SECTION_PBSMRTPIPE])
    all_cfg[OPTION_SECTION_PBSMRTPIPE] = pbsmrtpipe_opts

    # Dump all_cfg.
    dump_as_json(all_cfg, open(o_hgap_cfg_fn, 'w'))

    # Get logging cfg.
    logging_cfg = DEFAULT_LOGGING_CFG

    # Dump logging cfg.
    dump_as_json(logging_cfg, open(o_logging_cfg_fn, 'w'))
