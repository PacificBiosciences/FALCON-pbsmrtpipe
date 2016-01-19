"""Generate FALCON cfg (.ini file).

We plan to generate cfg in a complicated way.
But for now, we just use a look-up table,
based on ranges of the length of a genome.
"""
from falcon_kit import run_support as support
from . import tusks, reads
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
        'form01': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
    },
    'filters': {
    },
    'handlers': {
        'handler_file_pypeflow': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'form01',
            'filename': 'pypeflow.log',
        },
        'handler_file_hgap': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'form01',
            'filename': 'hgap.log',
        },
        'handler_stream': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'form01',
            'stream': 'ext://sys.stderr',
        },
    },
    'loggers': {
        'logger_pypeflow': {
            'level': 'NOTSET',
            'propagate': 1,
            'handlers': ['handler_file_pypeflow'],
            'qualname': 'pypeflow',
        },
        'logger_hgap': {
            'level': 'NOTSET',
            'propagate': 1,
            'handlers': ['handler_file_hgap'],
            'qualname': 'hgap',
            'foo': 'bar',
        },
    },
    'root': {
        'handlers': ['handler_stream'],
        'level': 'NOTSET',
    },
    'incremental': True,
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

def run_hgap_prepare(input_files, output_files, options):
    """Generate a config-file from options.
    """
    log.info('options to run_hgap_prepare:\n{}'.format(pprint.pformat(options)))
    i_subreadset_fn, = input_files
    o_hgap_cfg_fn, o_logging_cfg_fn = output_files
    run_dir = os.path.dirname(o_hgap_cfg_fn)

    # For now, ignore all but OPTION_CFG
    all_cfg = json.loads(options[TASK_HGAP_PREPARE_CFG])
    log.info('Parsed {!r}:\n{}'.format(
        TASK_HGAP_PREPARE_CFG, all_cfg))

    # Get options from pbsmrtpipe.
    pbsmrtpipe_opts = get_pbsmrtpipe_opts(run_dir)
    if OPTION_SECTION_PBSMRTPIPE not in all_cfg:
        all_cfg[OPTION_SECTION_PBSMRTPIPE] = dict()
    pbsmrtpipe_opts.update(all_cfg[OPTION_SECTION_PBSMRTPIPE])
    all_cfg[OPTION_SECTION_PBSMRTPIPE] = pbsmrtpipe_opts

    # Dump all_cfg.
    as_json = json.dumps(all_cfg, sort_keys=True, indent=4, separators=(',', ': '))
    log.info('Amended and formatted back to JSON:\n{}'.format(
        as_json))
    open(o_hgap_cfg_fn, 'w').write(as_json)

    # Dump logging cfg.
    logging_cfg = DEFAULT_LOGGING_CFG
    open(o_logging_cfg_fn, 'w').write(as_json)
