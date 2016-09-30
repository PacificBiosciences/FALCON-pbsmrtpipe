"""Generate FALCON cfg (.ini file).

We plan to generate cfg in a complicated way.
But for now, we just use a look-up table,
based on ranges of the length of a genome.
"""
from __future__ import absolute_import
from falcon_polish.functional import stricter_json
from falcon_polish.sys import symlink
from falcon_polish.pypeflow.hgap import update2
from falcon_kit import run_support as support
from . import gen_config # for some option names
import collections
import commands
import ConfigParser as configparser
import copy
import json
import logging
import os
import pprint
import re
import StringIO
import sys


#logging.basicConfig()
log = logging.getLogger(__name__)
#log.setLevel(logging.DEBUG)

TASK_HGAP_OPTIONS = \
        'falcon_ns.task_options.' + 'HGAP_Options_JSON'
TASK_HGAP_GENOME_LENGTH = \
        'falcon_ns.task_options.' + gen_config.OPTION_GENOME_LENGTH
TASK_HGAP_SEED_LENGTH_CUTOFF = \
        'falcon_ns.task_options.' + gen_config.OPTION_SEED_LENGTH_CUTOFF
TASK_HGAP_SEED_COVERAGE = \
        'falcon_ns.task_options.' + gen_config.OPTION_SEED_COVERAGE

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
        'handler_file_all': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'format_full',
            'filename': 'all.log',
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
    },
    'root': {
        'handlers': ['handler_stream', 'handler_file_all'],
        'level': 'NOTSET',
    },
    'disable_existing_loggers': False
}
"""
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
        }
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
"""
OPTION_SECTION_HGAP = 'hgap'
OPTION_SECTION_FALCON = 'falcon'
OPTION_SECTION_PBALIGN = 'pbalign'
OPTION_SECTION_VARIANTCALLER = 'variantcaller'
OPTION_SECTION_PBSMRTPIPE = 'pbsmrtpipe'

def say(msg):
    sys.stderr.write('Im just sayin:' + msg + '\n') # since pbsmrtpipe suppresses our logging
    log.info(msg)

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
    say('Dumping JSON:\n{}'.format(
        as_json))
    ofs.write(as_json)

def learn_job_type(ifs):
    content = ifs.read()
    re.sub(r'\\\s*', ' ', content)
    log.info('In learn_job_type(), content=\n"""\n{}\n"""'.format(content))
    try:
        return learn_job_type_from_new_cluster_sh(content)
    except Exception:
        log.exception('Apparently we are not using jsmcmd.')
    return learn_job_type_from_old_cluster_sh(content)
    # Can throw UnboundLocalError for missing queue_name

def learn_job_type_from_old_cluster_sh(content):
    """Infer the job_type from the pbsmrtpipe cluster.sh file.
    """
    re_qsub = re.compile(r'qsub\s+.+-q\s+(?P<queue>\S+)')
    mo = re_qsub.search(content)
    if not mo:
        raise Exception('re.search 1 failed for "{}" in "{}"'.format(
            re_qsub.pattern, content))
    job_type = 'sge'
    queue_name = mo.group('queue')
    return job_type, queue_name

def learn_jmsenv_ish_from_cluster_sh(content):
    re_qsub = re.compile(r'--jmsenv\s+"(?P<jmsenv>\S+)"', re.MULTILINE)
    mo = re_qsub.search(content)
    if not mo:
        raise Exception('re.search 2 failed for "{}" in "{}"'.format(
            re_qsub.pattern, content))
    jmsenv_ish = mo.group('jmsenv')
    return jmsenv_ish

def capture(cmd):
    log.info('capture(`{}`)'.format(cmd))
    status, output = commands.getstatusoutput(cmd)
    if status:
        raise Exception('Status: {} from cmd: `{}`'.format(status, output))
    return output

def learn_job_type_from_jmsenv_ish(output):
    re_qsub = re.compile(r'JMS_TYPE=(?P<jms_type>\S+)\s+QUEUE=(?P<queue>\S+)', re.MULTILINE)
    mo = re_qsub.search(output)
    if not mo:
        raise Exception('re.search 3 failed for "{}" in "{}"'.format(
            re_qsub.pattern, output))
    job_type = mo.group('jms_type').lower()
    queue_name = mo.group('queue')
    return job_type, queue_name


def learn_job_type_from_jmsenv(jmsenv_ish):
    bash = "bash -c '. {}; echo JMS_TYPE=$JMS_TYPE; echo QUEUE=$QUEUE'".format(jmsenv_ish)
    output = capture(bash)
    try:
        return learn_job_type_from_jmsenv_ish(output)
    except Exception:
        log.error('bash command was: {!r}'.format(bash))
        raise

def learn_job_type_from_new_cluster_sh(content):
    """Infer the job_type from the pbsmrtpipe cluster.sh file,
    which must use jmscmd.
    """
    try:
        jmsenv_ish = learn_jmsenv_ish_from_cluster_sh(content)
    except Exception:
        log.exception('Apparently we are not using jsmcmd. cluster.sh==\n"""\n{}\n"""'.format(content))
        raise
    return learn_job_type_from_jmsenv(jmsenv_ish)

def update_for_grid(all_cfg, run_dir):
    """Update both hgap and falcon options based
    on pbsmrtpipe cluster.sh file.
    Precondition: For now, all_cfg['falcon'] is empty or missing.
    """
    fc_cfg = all_cfg[OPTION_SECTION_FALCON]
    assert not fc_cfg
    cluster_sh_fn = os.path.join(run_dir, 'cluster.sh')
    if os.path.exists(cluster_sh_fn):
        with open(cluster_sh_fn) as ifs:
            job_type, queue_name = learn_job_type(ifs)
        all_cfg[OPTION_SECTION_HGAP]['job_type'] = job_type
        all_cfg[OPTION_SECTION_HGAP]['job_queue'] = queue_name
        sge_queue_option = ' -q {}'.format(queue_name)
        sge_option_names = (
                'sge_option_da', 'sge_option_la',
                'sge_option_pda', 'sge_option_pla',
                'sge_option_fc', 'sge_option_cns',
        )
        for option_name in sge_option_names:
            fc_cfg[option_name] = sge_queue_option + ' -pe smp 4' # TODO: Base on size/step.
    else:
        job_type = 'local'
        all_cfg[OPTION_SECTION_HGAP]['job_type'] = job_type
    # Note: The user should consider setting default_concurrent_jobs.

def update_falcon(all_cfg):
    use_tmpdir = all_cfg[OPTION_SECTION_HGAP].get('use_tmpdir')
    if use_tmpdir:
        all_cfg[OPTION_SECTION_FALCON] = use_tmpdir

def run_hgap_prepare(input_files, output_files, options):
    """Generate a config-file from options.
    """
    say('options to run_hgap_prepare:\n{}'.format(pprint.pformat(options)))
    i_subreadset_fn, = input_files
    o_hgap_cfg_fn, o_logging_cfg_fn, o_log_fn = output_files
    run_dir = os.path.dirname(o_hgap_cfg_fn)
    symlink(os.path.join(run_dir, 'stderr'), o_log_fn)

    # This will be the cfg we pass to hgap_run.
    all_cfg = collections.defaultdict(lambda: collections.defaultdict(str))

    # Get grid options, for job-distribution.
    update_for_grid(all_cfg, run_dir)

    # Set some other falcon options, based on hgap options.
    update_falcon(all_cfg)

    # Override from pbsmrtpipe config/preset.xml.
    all_cfg[OPTION_SECTION_FALCON]['genome_size'] = options[TASK_HGAP_GENOME_LENGTH].strip()
    all_cfg[OPTION_SECTION_FALCON]['length_cutoff'] = options[TASK_HGAP_SEED_LENGTH_CUTOFF].strip()
    all_cfg[OPTION_SECTION_FALCON]['seed_coverage'] = options[TASK_HGAP_SEED_COVERAGE].strip()
    cfg_json = options[TASK_HGAP_OPTIONS].strip()
    if not cfg_json:
        cfg_json = '{}'
    override_cfg = json.loads(stricter_json(cfg_json))
    update2(all_cfg, override_cfg)

    # Get options from pbsmrtpipe.
    pbsmrtpipe_opts = get_pbsmrtpipe_opts(run_dir)
    if OPTION_SECTION_PBSMRTPIPE not in all_cfg:
        all_cfg[OPTION_SECTION_PBSMRTPIPE] = dict()
    pbsmrtpipe_opts.update(all_cfg[OPTION_SECTION_PBSMRTPIPE])
    all_cfg[OPTION_SECTION_PBSMRTPIPE] = pbsmrtpipe_opts

    # Dump all_cfg.
    say('Dumping to {}'.format(repr(o_hgap_cfg_fn)))
    dump_as_json(all_cfg, open(o_hgap_cfg_fn, 'w'))

    # Get logging cfg.
    logging_cfg = DEFAULT_LOGGING_CFG

    # Dump logging cfg.
    say('Dumping to {}'.format(repr(o_logging_cfg_fn)))
    dump_as_json(logging_cfg, open(o_logging_cfg_fn, 'w'))
