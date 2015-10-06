###################
# FALCON TASKS
from .. import tusks as pbfalcon
from pbcommand.cli import registry_builder, registry_runner
from pbcommand.models import (FileTypes, OutputFileType)
import logging
import os
import sys
cd = pbfalcon.cd

log = logging.getLogger(__name__)

TOOL_NAMESPACE = 'falcon_ns'
DRIVER_BASE = "python -m pbfalcon.tasks.basic "

#from . import pbcommand_quick as pbquick
#registry = pbquick.registry_builder(TOOL_NAMESPACE, DRIVER_BASE)
registry = registry_builder(TOOL_NAMESPACE, DRIVER_BASE)

# FOFN = FileType(to_file_ns("generic_fofn"), "generic", "fofn", 'text/plain')
FC_FOFN = FileTypes.FOFN
FC_JSON = FileTypes.JSON
FC_CONFIG = FileTypes.TXT
FC_BASH = FileTypes.TXT
FC_DUMMY = FileTypes.TXT

def FT(file_type, basename):
    return OutputFileType(file_type.file_type_id,
                          "Label " + file_type.file_type_id,
                          repr(file_type),
                          "description for {f}".format(f=file_type),
                          basename)
RDJ = FT(FC_BASH, 'run_daligner_jobs.sh')

@registry('task_falcon_get_config', '0.0.0', [FC_FOFN], [FC_CONFIG],
          options=pbfalcon.get_config_default(),  is_distributed=False)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_falcon_get_config(rtc.task.input_files,
                                          rtc.task.output_files,
                                          rtc.task.options)

@registry('task_falcon_config_get_fasta', '0.0.0', [FC_CONFIG], [FC_FOFN], is_distributed=False)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_falcon_config_get_fasta(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon_config', '0.0.0', [FC_CONFIG, FC_FOFN], [FC_JSON], is_distributed=False)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_falcon_config(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon_make_fofn_abs', '0.0.0', [FC_FOFN], [FC_FOFN], is_distributed=False)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_falcon_make_fofn_abs(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_build_rdb', '0.0.0', [FC_JSON, FC_FOFN], [RDJ, FT(FC_DUMMY, 'job.done')], is_distributed=False)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_falcon_build_rdb(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon0_run_daligner_jobs', '0.0.0', [FC_JSON, RDJ], [FC_FOFN], is_distributed=True, nproc=4)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_daligner_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='raw_reads')

@registry('task_falcon0_run_merge_consensus_jobs', '0.0.0', [FC_JSON, RDJ, FC_FOFN], [FC_FOFN], is_distributed=True)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_merge_consensus_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='raw_reads')

# Run similar steps for preads.
@registry('task_falcon1_build_pdb', '0.0.0', [FC_JSON, FC_FOFN], [RDJ], is_distributed=False)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_falcon_build_pdb(rtc.task.input_files, rtc.task.output_files)

@registry('task_falcon1_run_daligner_jobs', '0.0.0', [FC_JSON, RDJ], [FC_FOFN], is_distributed=True, nproc=4)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_daligner_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='preads')

@registry('task_falcon1_run_merge_consensus_jobs', '0.0.0', [FC_JSON, RDJ, FC_FOFN], [FC_FOFN], is_distributed=True)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_merge_consensus_jobs(rtc.task.input_files, rtc.task.output_files, db_prefix='preads')

@registry('task_falcon2_run_asm', '0.0.0', [FC_JSON, FC_FOFN], [FileTypes.FASTA], is_distributed=False)
def run_rtc(rtc):
  with cd(os.path.dirname(rtc.task.output_files[0])):
    return pbfalcon.run_falcon_asm(rtc.task.input_files, rtc.task.output_files)


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
