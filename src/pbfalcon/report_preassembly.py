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
from __future__ import division
import sys
import os
import logging
import argparse
 
from pbcore.io import FastaReader
from pbsystem.common.cmdline.core import main_runner_default
from pbreports.model.model import Report, Attribute
from pbreports.util import get_fasta_readlengths, \
                        compute_n50_from_file
 
log = logging.getLogger(__name__)
 
__version__ = '0.1'
 
 
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
 
 
def to_report(filtered_subreads, filtered_longreads, corrected_reads, length_cutoff=None):
    """
    All inputs are paths to fasta files.
    """
    subreads = FastaContainer.from_file(filtered_subreads)
    longreads = FastaContainer.from_file(filtered_longreads)
    creads = FastaContainer.from_file(corrected_reads)
 
    fastas = [subreads, longreads, creads]
    for f in fastas:
        log.info(f)
 
    yield_ = creads.total / longreads.total
    rlength = int(creads.total / creads.nreads)
#    n50 = _compute_n50(corrected_reads, creads.total)
    n50 = compute_n50_from_file(corrected_reads)
 
    # Report Attributes
    attrs = []
    attrs.append(Attribute('polymerase_read_bases', value=subreads.total, name="Polymerase Read Bases"))
    if length_cutoff is not None:
        attrs.append(Attribute('length_cutoff', length_cutoff, name="Length Cutoff"))
    attrs.append(Attribute('seed_bases', longreads.total, name="Seed Bases"))
    attrs.append(Attribute('preassembled_bases', creads.total, name="Pre-Assembled bases"))
    attrs.append(Attribute('preassembled_yield', yield_, name="Pre-Assembled Yield"))
    attrs.append(Attribute('presssembled_reads', creads.nreads, name="Pre-Assembled Reads"))
    attrs.append(Attribute('presssembled_readlength', rlength, name="Pre-Assembled Reads Length"))
    attrs.append(Attribute('preassembled_n50', n50, name="Pre-Assembled N50"))
 
    report = Report('preassembly', attributes=attrs)
    return report
 
 
def args_runner(args):
    filtered_subreads = args.filtered_subreads_fasta
    filtered_longreads = args.filtered_longreads_fasta
    corrected_reads = args.corrected_reads
    length_cutoff = args.length_cutoff
    output_json = args.output_json
 
    log.info("Starting {f}".format(f=os.path.basename(__file__)))
    report = to_report(filtered_subreads, filtered_longreads, corrected_reads, length_cutoff=length_cutoff)
    log.info(report)
    with open(output_json, 'w') as f:
        log.info("Writing report to {r}.".format(r=output_json))
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
    p.add_argument('--genome-size', metavar="genome_size", type=int,
                   help="Size of genome.")
    p.add_argument("output_json", type=str, default="preassembly_report.json",
                   help="Path to Json Report output.")
 
    p.set_defaults(func=args_runner)
    return p
 
 
def main(argv=sys.argv):
    """Main point of Entry"""
    log.info("Starting {f} version {v} report generation".format(f=__file__, v=__version__))
    return main_runner_default(argv[1:], get_parser(), log)
 
 
if __name__ == '__main__':
    sys.exit(main())
