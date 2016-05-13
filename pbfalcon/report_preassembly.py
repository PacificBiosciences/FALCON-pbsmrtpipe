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
from __future__ import division
from pbcore.io import FastaReader
from pbcommand.models.report import Report, Attribute

import collections
import itertools
import json
import sys
import os
import logging
import argparse

log = logging.getLogger(__name__)
__version__ = '0.1'


# Copied from pbreports/util.py
# We want to avoid a dependency on pbreports b/c it needs matplotlib.
def get_fasta_readlengths(fasta_file):
    """
    Get a sorted list of contig lengths
    :return: (tuple)
    """
    lens = []
    with FastaReader(fasta_file) as f:
        for record in f:
            lens.append(len(record.sequence))
    lens.sort()
    return lens

def get_fasta_readlengths_a(fasta_file):
    """Assume we ran 'DBsplit -a',
    so we must ignore all but the short of any read.
    """
    lens = []
    zmw2len = dict()
    with FastaReader(fasta_file) as f:
        for record in f:
            l = len(record.sequence)
            zmw = record.id.split('/')[1]
            best_len = zmw2len.get(zmw)
            if best_len is None or best_len < l:
                zmw2len[zmw] = l
    lens = list(zmw2len.values())
    lens.sort()
    return lens

class FastaContainer(object):

    def __init__(self, nreads, total, file_name):
        self.nreads = nreads
        self.total = total
        self.file_name = file_name

    @staticmethod
    def from_file(file_name):
#        nreads, total = _compute_values(file_name)
        read_lens = get_fasta_readlengths(file_name)
        nreads = len(read_lens)
        total = sum(read_lens)
        return FastaContainer(nreads, total, file_name)

    def __str__(self):
        return "N {n} Total {t} File: {f}".format(n=self.nreads, t=self.total, f=self.file_name)

def _validate_file(file_name):
    if os.path.isfile(file_name):
        return os.path.abspath(file_name)
    else:
        msg = "Unable to find {f}".format(f=file_name)
        log.error(msg)
        raise IOError(msg)

def cutoff_reads(read_lens, min_read_len):
    return [rl for rl in read_lens if rl >= min_read_len]

def read_len_above(read_lens, threshold):
    subtotal = 0
    # Reverse-order calculation is faster.
    for irev, rl in enumerate(reversed(read_lens)):
        subtotal += rl
        if subtotal >= threshold:
            return rl

def percentile(read_lens, p):
    return read_lens[int(len(read_lens)*p)]

def stats_from_sorted_readlengths(read_lens):
    nreads = len(read_lens)
    total = sum(read_lens)
    n50 = read_len_above(read_lens, int(total * 0.50))
    p95 = percentile(read_lens, 0.95)
    #alt_n50 = pbreports.util.compute_n50(read_lens)
    #log.info('our n50=%s, pbreports=%s' %(n50, alt_n50)) # Ours is more correct when median is between 2 reads.
    stats = collections.namedtuple('FastaStats', ['nreads', 'total', 'n50', 'p95'])
    return stats(nreads=nreads, total=total, n50=n50, p95=p95)

def read_lens_from_fofn(fofn_fn):
    fns = [fn.strip() for fn in open(fofn_fn) if fn.strip()]
    # get_fasta_readlengths() returns sorted, so sorting the chain is roughly linear.
    return list(sorted(itertools.chain.from_iterable(get_fasta_readlengths(fn) for fn in fns)))

def read_lens_from_fofn_a(fofn_fn):
    """Assume we ran 'DBsplit -a',
    so we must ignore all but the short of any read.
    """
    fns = [fn.strip() for fn in open(fofn_fn) if fn.strip()]
    # get_fasta_readlengths() returns sorted, so sorting the chain is roughly linear.
    return list(sorted(itertools.chain.from_iterable(get_fasta_readlengths_a(fn) for fn in fns)))

def _get_length_cutoff_from_somewhere(length_cutoff, tasks_dir):
    if length_cutoff < 0:
        fn = os.path.join(tasks_dir, 'falcon_ns.tasks.task_falcon0_build_rdb-0', 'length_cutoff')
        try:
            length_cutoff = int(open(fn).read().strip())
            log.info('length_cutoff=%d from %r' %(length_cutoff, fn))
        except Exception:
            log.exception('Unable to read length_cutoff from "%s".' %fn)
    return length_cutoff # possibly updated

def _get_cfg(i_json_config_fn):
    import pprint
    cfg = json.loads(open(i_json_config_fn).read())
    log.info('cfg=\n%s' %pprint.pformat(cfg))
    length_cutoff = int(cfg.get('length_cutoff', '0'))
    length_cutoff = _get_length_cutoff_from_somewhere(length_cutoff, os.path.dirname(os.path.dirname(i_json_config_fn)))
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
    cfg = _get_cfg(i_json_config_fn)
    genome_length = int(cfg.get('genome_size', 0)) # different name in falcon
    length_cutoff = cfg['length_cutoff']

    raw_reads = read_lens_from_fofn(i_raw_reads_fofn_fn)
    stats_raw_reads = stats_from_sorted_readlengths(raw_reads)
    del raw_reads

    uniq_raw_reads = read_lens_from_fofn_a(i_raw_reads_fofn_fn)
    seed_reads = cutoff_reads(uniq_raw_reads, length_cutoff)
    del uniq_raw_reads
    stats_seed_reads = stats_from_sorted_readlengths(seed_reads)

    preads = read_lens_from_fofn(i_preads_fofn_fn)
    stats_preads = stats_from_sorted_readlengths(preads)

    report = to_report(
            stats_raw_reads=stats_raw_reads,
            stats_seed_reads=stats_seed_reads,
            stats_corrected_reads=stats_preads,
            genome_length=genome_length,
            length_cutoff=length_cutoff,
    )
    log.info('%r -> %r' %(report, o_json_fn))
    with open(o_json_fn, 'w') as ofs:
        log.info("Writing report to {!r}.".format(o_json_fn))
        content = report.to_json()
        ofs.write(content)

def to_report(stats_raw_reads, stats_seed_reads, stats_corrected_reads, genome_length=None, length_cutoff=None):
    """All inputs are paths to fasta files.
    """
    log.info('stats for raw reads:       %s' %repr(stats_raw_reads))
    log.info('stats for seed reads:      %s' %repr(stats_seed_reads))
    log.info('stats for corrected reads: %s' %repr(stats_corrected_reads))

    kwds = {}
    kwds['genome_length'] = 0 if genome_length is None else genome_length
    kwds['length_cutoff'] = 0 if length_cutoff is None else length_cutoff
    kwds['raw_reads'] = stats_raw_reads.nreads
    kwds['raw_bases'] = stats_raw_reads.total
    kwds['raw_mean'] = stats_raw_reads.total / stats_raw_reads.nreads
    kwds['raw_n50'] = stats_raw_reads.n50
    kwds['raw_p95'] = stats_raw_reads.p95
    kwds['raw_coverage'] = stats_raw_reads.total / genome_length
    kwds['seed_reads'] = stats_seed_reads.nreads
    kwds['seed_bases'] = stats_seed_reads.total
    kwds['seed_mean'] = stats_seed_reads.total / stats_seed_reads.nreads
    kwds['seed_n50'] = stats_seed_reads.n50
    kwds['seed_p95'] = stats_seed_reads.p95
    kwds['seed_coverage'] = stats_seed_reads.total / genome_length
    kwds['preassembled_reads'] = stats_corrected_reads.nreads
    kwds['preassembled_bases'] = stats_corrected_reads.total
    kwds['preassembled_mean'] = stats_corrected_reads.total / stats_corrected_reads.nreads
    kwds['preassembled_n50'] = stats_corrected_reads.n50
    kwds['preassembled_p95'] = stats_corrected_reads.p95
    kwds['preassembled_coverage'] = stats_corrected_reads.total / genome_length
    kwds['preassembled_yield'] = stats_corrected_reads.total / stats_seed_reads.total
    return produce_report(**kwds)

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


def args_runner(args):
    filtered_subreads = args.filtered_subreads_fasta
    filtered_longreads = args.filtered_longreads_fasta
    corrected_reads = args.corrected_reads
    length_cutoff = args.length_cutoff
    genome_length = args.genome_length
    output_json = args.output_json

    log.info("Starting {f}".format(f=os.path.basename(__file__)))

    raw_reads = read_lens_from_fofn(filtered_subreads)
    stats_raw_reads = stats_from_sorted_readlengths(raw_reads)

    seed_reads = read_lens_from_fofn(filtered_longreads)
    stats_seed_reads = stats_from_sorted_readlengths(seed_reads)

    preads = read_lens_from_fofn(corrected_reads)
    stats_preads = stats_from_sorted_readlengths(preads)

    report = to_report(
            stats_raw_reads=stats_raw_reads,
            stats_seed_reads=stats_seed_reads,
            stats_corrected_reads=stats_preads,
            genome_length=genome_length,
            length_cutoff=length_cutoff,
    )
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
