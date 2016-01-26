from __future__ import absolute_import
import argparse
import copy
import json
import logging
import logging.config
import os
import pprint
import sys
import time
import ConfigParser as configparser
import StringIO
log = logging.getLogger(__name__)

default_logging_config = """
[loggers]
keys=root,pypeflow,fc_run

[handlers]
keys=stream,file_pypeflow,file_fc

[formatters]
keys=form01,form02

[logger_root]
level=NOTSET
handlers=stream

[logger_pypeflow]
level=DEBUG
handlers=file_pypeflow
qualname=pypeflow
propagate=1

[logger_fc_run]
level=NOTSET
handlers=file_fc
qualname=.
propagate=1

[handler_stream]
class=StreamHandler
level=INFO
formatter=form02
args=(sys.stderr,)

[handler_file_pypeflow]
class=FileHandler
level=INFO
formatter=form01
args=('pypeflow.log',)

[handler_file_fc]
class=FileHandler
level=DEBUG
formatter=form01
args=('fc.log',)

[formatter_form01]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s

[formatter_form02]
format=[%(levelname)s]%(message)s
"""

dl = \
{
    'version': 1,
    'disable_existing_loggers': True,
    #'incremental': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        },
        'django.request': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': False
        },
    }
}
def setup_logger(logging_config_fn):
    """See https://docs.python.org/2/library/logging.config.html
    """
    logging.Formatter.converter = time.gmtime # cannot be done in .ini

    #logging.config.dictConfig(dl)
    #return
    if logging_config_fn:
        if logging_config_fn.endswith('.json'):
            logging.config.dictConfig(json.loads(open(logging_config_fn).read()))
            return
        logger_fileobj = open(logging_config_fn)
    else:
        logger_fileobj = StringIO.StringIO(default_logging_config)
    defaults = {
    }
    logging.config.fileConfig(logger_fileobj, defaults=defaults, disable_existing_loggers=False)

def cfg2dict(ifp):
    cp = configparser.ConfigParser()
    cp.readfp(ifp)
    return {section: cp.items(section) for section in cp.sections()}

DEFAULT_OPTIONS = """
{
  "hgap": {
    "SuppressAuto": true,
    "GenomeSize": 8000,
    "min_length_cutoff": 1,
    "~comment": "Overrides for full HGAP pipeline"
  },
  "falcon": {
    "falcon_sense_option": "--output_multi --min_idt 0.77 --min_cov 10 --max_n_read 2000 --n_core 6",
    "length_cutoff": "1",
    "length_cutoff_pr": "1",
    "overlap_filtering_setting": "--max_diff 1000 --max_cov 100000 --min_cov 0 --bestn 1000 --n_core 4",
    "ovlp_DBsplit_option": "-s50 -a",
    "ovlp_HPCdaligner_option": "-v -k15 -h60 -w5 -H1 -e.95 -l40 -s100 -M4",
    "ovlp_concurrent_jobs": "32",
    "pa_DBsplit_option": "-x250 -s500 -a",
    "pa_HPCdaligner_option": "-v -k15 -h35 -w5 -H1 -e.70 -l40 -s100 -M4",
    "pa_concurrent_jobs": "32",
    "~comment": "Overrides for FALCON"
  },
  "pbalign": {
    "options": "--hitPolicy randombest --minAccuracy 70.0 --minLength 50 --algorithm=blasr --concordant",
    "algorithmOptions": "-minMatch 12 -bestn 10 -minPctSimilarity 70.0",
    "_jdnotes": "--maxHits 1 --minAnchorSize 12 --maxDivergence=30 --minAccuracy=0.75 --minLength=50 --hitPolicy=random --seed=1",
    "~comment": "Overrides for blasr alignment (prior to polishing)"
  },
  "variantCaller": {
    "options": "--algorithm quiver --diploid --min_confidence 40 --min_coverage 5",
    "~comment": "Overrides for genomic consensus (polishing)"
  },
  "pbsmrtpipe": {
    "~comment": "Overrides for pbsmrtpipe"
  },
  "~comment": "https://github.com/PacificBiosciences/ExperimentalPipelineOptionsDocs/HGAP"
}
"""
DEFAULT_OPTIONS = json.loads(DEFAULT_OPTIONS)

def update2(start, updates):
    for key1, val1 in updates.iteritems():
        if key1 not in start:
            start[key1] = copy.deepcopy(val1)
            continue
        assert isinstance(start[key1], dict)
        for key2, val2 in val1.iteritems():
            start[key1][key2] = copy.deepcopy(val2)

def main1(input_config_fn, logging_config_fn=None):
    global log
    setup_logger(logging_config_fn)
    log = logging.getLogger(__name__)
    log.info('Read logging config from {!r}'.format(logging_config_fn))
    log.info('Reading HGAP config from {!r}'.format(input_config_fn))
    if input_config_fn.endswith('.json'):
        config = json.loads(open(input_config_fn).read())
    else:
        config = cfg2dict(open(input_config_fn))
    log.info('config=\n{}'.format(pprint.pformat(config)))
    log.info('defs=\n{}'.format(pprint.pformat(DEFAULT_OPTIONS)))
    opts = dict()
    opts.update(DEFAULT_OPTIONS)
    update2(opts, config)
    log.info('opts=\n{}'.format(pprint.pformat(opts)))

def main(argv=sys.argv):
    print(argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--logging',
            help='.ini or .json config file for Python logging module')
    parser.add_argument('config',
            help='.ini or .json of HGAP config. Available sections: "general", "hgap", "falcon", "pbsmrtpipe", "blasr", "quiver", ...')
    args = parser.parse_args(argv[1:])
    return main1(args.config, args.logging)

if __name__ == "__main__":
    main(sys.argv)
