import argparse
import json
import logging
import logging.config
import os
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

def setup_logger(logging_config_fn):
    """See https://docs.python.org/2/library/logging.config.html
    """
    logging.Formatter.converter = time.gmtime # cannot be done in .ini

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
