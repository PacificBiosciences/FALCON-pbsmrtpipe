#!/usr/bin/env python
""" PreAssembly Report.

Output of Original Report

<?xml version="1.0" encoding="UTF-8"?>
<report>
  <layout onecolumn="true"/>
  <title>Pre-Assembly</title>
  <attributes>
    <attribute id="1" name="Polymerase Read Bases" value="125856600" hidden="true">125856600</attribute>
    <attribute id="2" name="Length Cutoff" value="6000" hidden="true">6000</attribute>
    <attribute id="3" name="Seed Bases" value="18610" hidden="true">18610</attribute>
    <attribute id="4" name="Pre-Assembled Bases" value="3628" hidden="true">3628</attribute>
    <attribute id="5" name="Pre-Assembled Yield" value=".194" hidden="true">.194</attribute>
    <attribute id="6" name="Pre-Assembled Reads" value="3" hidden="true">3</attribute>
    <attribute id="7" name="Pre-Assembled Read Length" value="1209" hidden="true">1209</attribute>
    <attribute id="8" name="Pre-Assembled N50" value="1300" hidden="true">1300</attribute>
  </attributes>
</report>
"""
# Copied from
#   http://swarm/files/depot/branches/springfield/S2.3/software/smrtanalysis/bioinformatics/tools/pbreports/pbreports/report/preassembly.py
from __future__ import absolute_import
from pbcommand.models.report import Report, Attribute
from falcon_polish.functional import stricter_json
from falcon_kit.run_support import get_length_cutoff
from falcon_kit import stats_preassembly
import argparse
import json
import logging
import os
import pprint
import sys

log = logging.getLogger(__name__)
__version__ = '0.1'


def _get_cfg(i_json_config_fn, i_length_cutoff_fn):
    cfg = json.loads(stricter_json(open(i_json_config_fn).read()))
    log.info('cfg=\n%s' %pprint.pformat(cfg))
    length_cutoff = int(cfg.get('length_cutoff', '0'))
    length_cutoff = get_length_cutoff(length_cutoff, i_length_cutoff_fn)
    cfg['length_cutoff'] = length_cutoff
    return cfg

def for_task(
        i_json_config_fn,
        i_preads_fofn_fn,
        i_raw_reads_fofn_fn,
        o_json_fn,
    ):
    """See pbfalcon.tusks
    """
    tasks_dir = os.path.dirname(os.path.dirname(i_json_config_fn))
    i_length_cutoff_fn = os.path.join(tasks_dir, 'falcon_ns.tasks.task_falcon0_build_rdb-0', 'length_cutoff')
    cfg = _get_cfg(i_json_config_fn, i_length_cutoff_fn)
    genome_length = int(cfg.get('genome_size', 0)) # different name in falcon
    length_cutoff = cfg['length_cutoff']

    report_dict = stats_preassembly.make_dict(
        i_preads_fofn_fn,
        i_raw_reads_fofn_fn,
        genome_length,
        length_cutoff,
    )
    report = produce_report(**report_dict)
    log.info('%r -> %r' %(report, o_json_fn))
    with open(o_json_fn, 'w') as ofs:
        log.info("Writing report to {!r}.".format(o_json_fn))
        content = report.to_json()
        ofs.write(content)

def produce_report(
        genome_length,
        raw_reads,
        raw_mean,
        raw_n50,
        raw_p95,
        raw_bases,
        raw_coverage,
        length_cutoff,
        seed_reads,
        seed_bases,
        seed_mean,
        seed_n50,
        seed_p95,
        seed_coverage,
        preassembled_reads,
        preassembled_mean,
        preassembled_n50,
        preassembled_p95,
        preassembled_bases,
        preassembled_coverage,
        preassembled_yield,
        **ignored
    ):
    #preassembled_yield = '{:.3f}'.format(preassembled_yield) # but this would make it a str, unlike the others.
    # Report Attributes
    attrs = []
    attrs.append(Attribute('genome_length', genome_length, name="Genome Length (user input)"))
    attrs.append(Attribute('raw_reads', raw_reads, name="Number of Raw Reads"))
    attrs.append(Attribute('raw_mean', raw_mean, name="Raw Read Length Mean"))
    attrs.append(Attribute('raw_n50', raw_n50, name="Raw Read Length (N50)"))
    attrs.append(Attribute('raw_p95', raw_p95, name="Raw Read Length 95%"))
    attrs.append(Attribute('raw_bases', raw_bases, name="Number of Raw Bases (total)"))
    attrs.append(Attribute('raw_coverage', raw_coverage, name="Raw Coverage (bases/genome_size)"))
    attrs.append(Attribute('length_cutoff', length_cutoff, name="Length Cutoff (user input or auto-calc)"))
    attrs.append(Attribute('seed_reads', seed_reads, name="Number of Seed Reads"))
    attrs.append(Attribute('seed_mean', seed_mean, name="Seed Read Length Mean"))
    attrs.append(Attribute('seed_n50', seed_n50, name="Seed Read Length (N50)"))
    attrs.append(Attribute('seed_p95', seed_p95, name="Seed Read Length 95%"))
    attrs.append(Attribute('seed_bases', seed_bases, name="Number of Seed Bases (total)"))
    attrs.append(Attribute('seed_coverage', seed_coverage, name="Seed Coverage (bases/genome_size)"))
    attrs.append(Attribute('preassembled_reads', preassembled_reads, name="Number of Pre-Assembled Reads"))
    attrs.append(Attribute('preassembled_mean', preassembled_mean, name="Pre-Assembled Read Length Mean"))
    attrs.append(Attribute('preassembled_n50', preassembled_n50, name="Pre-Assembled Read Length (N50)"))
    attrs.append(Attribute('preassembled_p95', preassembled_p95, name="Pre-Assembled Read Length 95%"))
    attrs.append(Attribute('preassembled_bases', preassembled_bases, name="Number of Pre-Assembled Bases (total)"))
    attrs.append(Attribute('preassembled_coverage', preassembled_coverage, name="Pre-Assembled Coverage (bases/genome_size)"))
    attrs.append(Attribute('preassembled_yield', preassembled_yield, name="Pre-Assembled Yield (bases/seed_bases)"))

    report = Report('preassembly', title='Pre-assembly', attributes=attrs)
    return report

def write_report_from_stats(stats_ifs, report_ofs):
    stats = json.loads(stricter_json(stats_ifs.read()))
    report = produce_report(**stats)
    content = report.to_json()
    report_ofs.write(content)

def args_runner(args):
    # UNTESTED -- but never used anyway
    log.info("Starting {f}".format(f=os.path.basename(__file__)))
    filtered_subreads = args.filtered_subreads_fasta
    filtered_longreads = args.filtered_longreads_fasta #???
    corrected_reads = args.corrected_reads
    length_cutoff = args.length_cutoff
    genome_length = args.genome_length
    output_json = args.output_json
    cfg = {
        'length_cutoff': length_cutoff,
    }

    report_dict = falcon_polish.stats_preassembly.make_dict(
        corrected_reads,
        filtered_subreads,
        genome_length,
        length_cutoff,
    )
    report = produce_report(**report_dict)
    log.info('%r -> %r' %(report, o_json_fn))

    log.info(report)
    with open(output_json, 'w') as f:
        log.info("Writing report to {!r}.".format(output_json))
        f.write(report.to_json())

    return 0


def get_parser():
    p = argparse.ArgumentParser(version=__version__)
    p.add_argument('filtered_subreads_fasta', type=_validate_file,
                   help="Path to filtered reads.")
    p.add_argument('filtered_longreads_fasta', type=_validate_file,
                   help="Path to filtered longreads.")
    p.add_argument("corrected_reads", type=_validate_file,
                   help="Path to corrected reads.")
    p.add_argument("--debug", action='store_true',
                   help="Flag to debug to stdout.")
    p.add_argument('--length-cutoff', type=int, metavar="length_cutoff",
                   help="Length cutoff to insert into report.")
    p.add_argument('--genome-length', metavar="genome_length", type=int,
                   help="Size of genome (base pairs).")
    p.add_argument("output_json", type=str, default="preassembly_report.json",
                   help="Path to Json Report output.")

    p.set_defaults(func=args_runner)
    return p


def main(argv=sys.argv):
    """Main point of Entry"""
    log.info("Starting {f} version {v} report generation".format(f=__file__, v=__version__))
    parser = get_parser()
    args = parser.parse_args(argv[1:])
    return args_runner(args)


if __name__ == '__main__':
    sys.exit(main())
