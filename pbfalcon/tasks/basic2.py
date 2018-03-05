###################
# FALCON TASKS
# pylint: disable=function-redefined
from .. import runners as pbfalcon
from pbcommand.cli import registry_builder, registry_runner, QuickOpt
from pbcommand.models import (FileTypes, OutputFileType, ResourceTypes)
import logging
import os
import StringIO
import sys
cd = pbfalcon.cd

log = logging.getLogger(__name__)

TOOL_NAMESPACE = 'falcon_ns2'
DRIVER_BASE = "python -m pbfalcon.tasks.basic2 "
#DRIVER_BASE = "/mnt/software/p/python/2.7.14-UCS4/bin/python -m pbfalcon.tasks.basic2 "

#from . import pbcommand_quick as pbquick
#registry = pbquick.registry_builder(TOOL_NAMESPACE, DRIVER_BASE)
registry = registry_builder(TOOL_NAMESPACE, DRIVER_BASE)

# FT_FOFN = FileType(to_file_ns("generic_fofn"), "generic", "fofn", 'text/plain')
FT_FOFN = FileTypes.FOFN
FT_TXT = FileTypes.TXT
FT_JSON = FileTypes.JSON
FT_CFG = FileTypes.CFG
FT_BASH = FileTypes.TXT
FT_DUMMY = FileTypes.TXT
FT_SUBREADS = FileTypes.DS_SUBREADS
FT_CONTIGS = FileTypes.DS_CONTIG
FT_FASTA = FileTypes.FASTA
FT_REPORT = FileTypes.REPORT
FT_LOG = FileTypes.LOG

def FT(file_type, basename, title):
    # (file_type_id, label, display_name, description, default_name)
    return OutputFileType(file_type.file_type_id,
                          basename + '_id',
                          title,
                          "description for {f}".format(f=file_type),
                          basename)
FT_DB = FT(FT_DUMMY, 'dazzler.db', "Database Overview")
FT_JSON_OUT = OutputFileType(FileTypes.JSON.file_type_id,
                             "json_id",
                             "JSON",
                             "Generic JSON file",
                             "file")
FT_FASTA_OUT = OutputFileType(FileTypes.FASTA.file_type_id,
                              "fasta_id",
                              "FASTA",
                              "FASTA sequences",
                              "reads")
FT_CONTIGS_OUT = OutputFileType(FileTypes.DS_CONTIG.file_type_id,
                              "contig_id",
                              "contigset",
                              "Contigset of polished FASTA sequences",
                              "polished.contigset")


FT_FOFN_OUT = OutputFileType(FileTypes.FOFN.file_type_id,
                             "fofn_id",
                             "FOFN of daligner input (.fasta paths, possibly relative)",
                             "file of file names of fasta input",
                             "file")
@registry('task_falcon_config_get_fasta', '0.0.0', [FT_CFG], [FT_FOFN_OUT], is_distributed=False)
def run_rtc(rtc): #KEEP for later
    return pbfalcon.run_falcon_config_get_fasta(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon_config', '0.0.0', [FT_CFG, FT_FOFN], [FT_JSON_OUT], is_distributed=False)
def run_rtc(rtc):
    return pbfalcon.run_falcon_config(rtc.task.input_files, rtc.task.output_files)

FT_FOFN_OUT = OutputFileType(FileTypes.FOFN.file_type_id,
                             "fofn_id",
                             "FOFN of daligner input (absolute .fasta paths)",
                             "file of file names of fasta input",
                             "file")
@registry('task_falcon_make_fofn_abs', '0.0.0', [FT_JSON], [FT_FOFN_OUT], is_distributed=False)
def run_rtc(rtc):
    return pbfalcon.run_falcon_make_fofn_abs(rtc.task.input_files, rtc.task.output_files)

RDJ0_OUT = OutputFileType(FileTypes.TXT.file_type_id,
                             "run_daligner_jobs0_id",
                             "bash file from HPC.daligner, stage-0",
                             "bash script",
                             "run_daligner_jobs0.sh")
@registry('task_falcon0_build_rdb', '0.0.0', [FT_JSON, FT_FOFN], [RDJ0_OUT, FT_DB, FT(FT_DUMMY, 'job.done', "Status file"), FT(FT_TXT, 'length_cutoff', 'Just a number, the length-cutoff for raw_reads to consider, which might be provided or might be generated')], is_distributed=True)
def run_rtc(rtc):
    return pbfalcon.run_falcon_build_rdb(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_run_daligner_split', '0.0.0', [RDJ0_OUT, FT_DB], [FT(FT_JSON, 'all-units-of-work', 'daligner run units from HPC.daligner'), FT(FT_TXT, 'daligner_bash_template.sh', 'bash run script')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_daligner_split(rtc.task.input_files, rtc.task.output_files, db_prefix='raw_reads')

@registry('task_falcon0_run_daligner_jobs', '0.0.0', [FT_JSON, FT_TXT], [FT(FT_JSON, 'results', 'for now, just done-sentinels')], is_distributed=True, nproc=4)
def run_rtc(rtc):
    return pbfalcon.run_generic_chunkable_jobs(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_run_daligner_find_las', '0.0.0', [FT_JSON], [FT(FT_JSON, 'gathered-las', '*.las files that were next to the done-sentinels (as we did not know their names in advance)')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_daligner_find_las(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_run_las_merge_split', '0.0.0', [RDJ0_OUT, FT_JSON], [FT(FT_JSON, 'all-units-of-work', 'Split work script for LAmerge'), FT(FT_TXT, 'daligner_bash_template.sh', 'bash run script')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_las_merge_split(rtc.task.input_files, rtc.task.output_files, db_prefix='raw_reads')

@registry('task_falcon0_run_las_merge_jobs', '0.0.0', [FT_JSON, FT_TXT], [FT(FT_JSON, 'results', 'from each LAmerge job')], is_distributed=True, nproc=4)
def run_rtc(rtc):
    return pbfalcon.run_generic_chunkable_jobs(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_rm_las', '0.0.0', [FT_JSON], [FT_TXT], is_distributed=True)
# remove raw_reads.*.raw_reads.*.las # TODO: These should already be gone.
def run_rtc(rtc):
    return pbfalcon.run_rm_las(rtc.task.input_files, rtc.task.output_files, prefix='L*.*.raw_reads.')

@registry('task_falcon0_run_las_merge_post_gather', '0.0.0', [FT_JSON], [FT(FT_JSON, 'las-fofn', 'JSON list of .las filenames'), FT(FT_JSON, 'p_id2las', 'Odd file, currently consumed by consensus.')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_las_merge_post_gather(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_run_cns_split', '0.0.0', [FT_JSON, FT_DB, FT_JSON, FT_TXT], [FT(FT_JSON, 'all-units-of-work', 'Split work for consensus'), FT(FT_TXT, 'daligner_bash_template.sh', 'bash run script')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_cns_split(rtc.task.input_files, rtc.task.output_files)

# Typically, 6 procs for falcon_sense, but really that is set in cfg.
# We run each block on a single machine because we currently use python 'multiproc'.
# We run one 6-proc job for each block, serially.
# Too many could swamp NFS, so serial on one machine is fine, for now, until we measure.
# We pipe the result of LA4Falcon to the main process, which means that each fork consumes that much memory;
# that is the main impact on other processes on the same machine, typically 6GB altogether.
# Because this is I/O bound, we do not really harm the machine we are on,
# but we need to reserve some memory. nproc=6 is more than enough.
# TODO: Move into /tmp, to reduce the burden on NFS.
@registry('task_falcon0_run_cns_jobs', '0.0.0', [FT_JSON, FT_TXT], [FT(FT_JSON, 'results', 'from each consensus job')], is_distributed=True, nproc=6)
def run_rtc(rtc):
    return pbfalcon.run_generic_chunkable_jobs(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_rm_las', '0.0.0', [FT_JSON, FT_TXT], [FT_TXT], is_distributed=True)
# 2nd input is dummy, for ordering
def run_rtc(rtc):
    # Remove initial files in the case that DALIGNER decided to name them by prefix, rather than
    # by L1.*. I think that happens when no intermediate level is needed.
    return pbfalcon.run_rm_las(rtc.task.input_files, rtc.task.output_files, prefix='raw_reads.*.raw_reads.')

@registry('task_falcon0_run_cns_post_gather', '0.0.0', [FT_JSON], [FT(FT_FOFN, 'input-preads', 'Simple list of .fasta filenames of preads')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_cns_post_gather(rtc.task.input_files, rtc.task.output_files)

@registry('task_report_preassembly_yield', '0.0.0', [FT_JSON, FT_FOFN, FT_DB, FT_TXT], [FT(FT_REPORT, 'preassembly_yield', "Preassembly report")], is_distributed=False)
def run_rtc(rtc):
    return pbfalcon.run_report_preassembly_yield(rtc.task.input_files, rtc.task.output_files)

# Run similar steps for preads.
RDJ1_OUT = OutputFileType(FileTypes.TXT.file_type_id,
                          "run_daligner_jobs1_id",
                          "bash file from HPC.daligner, stage-1",
                          "bash script",
                          "run_daligner_jobs1.sh")
@registry('task_falcon1_build_pdb', '0.0.0', [FT_JSON, FT_FOFN], [RDJ1_OUT, FT_DB, FT(FT_DUMMY, 'job.done', "Status file")], is_distributed=True)
def run_rtc(rtc):
    return pbfalcon.run_falcon_build_pdb(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_run_daligner_split', '0.0.0', [RDJ1_OUT, FT_DB], [FT(FT_JSON, 'all-units-of-work', 'daligner run units from HPC.daligner'), FT(FT_TXT, 'daligner_bash_template.sh', 'bash run script')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_daligner_split(rtc.task.input_files, rtc.task.output_files, db_prefix='preads')

@registry('task_falcon1_run_daligner_jobs', '0.0.0', [FT_JSON, FT_TXT], [FT(FT_JSON, 'results', 'for now, just done-sentinels')], is_distributed=True, nproc=4)
def run_rtc(rtc):
    return pbfalcon.run_generic_chunkable_jobs(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_run_daligner_find_las', '0.0.0', [FT_JSON], [FT(FT_JSON, 'gathered-las', '*.las files that were next to the done-sentinels (as we did not know their names in advance)')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_daligner_find_las(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_run_las_merge_split', '0.0.0', [RDJ0_OUT, FT_JSON], [FT(FT_JSON, 'all-units-of-work', 'Split work script for LAmerge'), FT(FT_TXT, 'daligner_bash_template.sh', 'bash run script')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_las_merge_split(rtc.task.input_files, rtc.task.output_files, db_prefix='preads')

@registry('task_falcon1_run_las_merge_jobs', '0.0.0', [FT_JSON, FT_TXT], [FT(FT_JSON, 'results', 'from each LAmerge job')], is_distributed=True, nproc=4)
def run_rtc(rtc):
    return pbfalcon.run_generic_chunkable_jobs(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_run_las_merge_post_gather', '0.0.0', [FT_JSON], [FT(FT_JSON, 'las-fofn', 'JSON list of .las filenames'), FT(FT_JSON, 'p_id2las', 'Odd file, currently consumed (implicitly) by db2falcon.')], is_distributed=False, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_las_merge_post_gather(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_run_db2falcon', '0.0.0', [FT_JSON, FT_DB], [FT(FT_FASTA, 'preads4falcon', 'Input to falcon assembly'), FT(FT_TXT, 'db2falcon-done', 'Sentinel, which we can drop soon hopefully.')], is_distributed=True, nproc=6)
def run_rtc(rtc):
    return pbfalcon.run_db2falcon(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon2_run_falcon_asm', '0.0.0', [FT_FASTA, FT_DB, FT_JSON, FT_JSON, FT_TXT], [FT(FT_FASTA, 'p_ctg', 'Primary contigs of draft assembly')], is_distributed=True, nproc=1)
def run_rtc(rtc):
    return pbfalcon.run_falcon_asm(rtc.task.input_files, rtc.task.output_files)

opt_save_unzip = QuickOpt(False, "Save Output for Unzip",
    "Saves certain files that enable running unzip via command line")

@registry('task_falcon2_rm_las', '0.0.0', [FT_FASTA, FT_TXT], [FT_TXT], options={'save_las_for_unzip':opt_save_unzip}, is_distributed=True)
# 2nd input is dummy, for ordering
def run_rtc(rtc):
    optname = "falcon_ns.task_options.save_las_for_unzip"
    if not bool(rtc.task.options.get(optname, False)):
      # remove all .las files
      return pbfalcon.run_rm_las(rtc.task.input_files, rtc.task.output_files, prefix='')
    else:
      # remove intermediate pread .las files (both raw and pread final merged/sorted .las files remain)
      return pbfalcon.run_rm_las(rtc.task.input_files, rtc.task.output_files, prefix='preads.*.preads.')

if __name__ == '__main__':
    from falcon_kit import run_support
    run_support.logger = logging.getLogger("fc_run")
    sys.exit(registry_runner(registry, sys.argv[1:]))
