all:
	pip install -v -e .
	pb-falcon -h
run-dev:
	cd dev_01; pbsmrtpipe  pipeline $$(pwd)/workflow_id.xml --debug  -e e_01:$$(pwd)/input.txt --preset-xml=$$(pwd)/preset.xml    --output-dir=$$(pwd)/job_output
run:
	cd falcon; pbsmrtpipe  pipeline $$(pwd)/workflow_id.xml --debug  -e e_01:$$(pwd)/input.txt --preset-xml=$$(pwd)/preset.xml    --output-dir=$$(pwd)/job_output

utest:
	py.test -v utest/

PBFALCON_TC_RUNNERS:= \
	pbfalcon.cli.task_gen_config \
	pbfalcon.cli.task_hgap_prepare \
	pbfalcon.cli.task_hgap_run \
	pbfalcon.tasks.scatter0_run_daligner_jobs \
	pbfalcon.tasks.scatter1_run_daligner_jobs \
	pbfalcon.tasks.gather0_run_daligner_jobs \
	pbfalcon.tasks.gather1_run_daligner_jobs \
	pbfalcon.tasks.scatter_run_scripts_in_json \
	pbfalcon.tasks.scatter_run_scripts_in_json_2 \

update:
	\rm -f *.json
	${MAKE} modules runners
	${MAKE} canon
canon:
	./canonicalize.py *.json
install:
	mv *.json ../pbsmrtpipe/pbsmrtpipe/registered_tool_contracts_sa3
modules:
	python -m pbfalcon.tasks.basic emit-tool-contracts
runners: ${PBFALCON_TC_RUNNERS}

# This applies only to pbcommand-runners, not plain modules.
%:
	python -m $@ --emit-tool-contract >| $@_tool_contract.json

# Someday, to hide options?
# //depot/software/smrtanalysis/services-ui/scala/services/secondary-smrt-server/src/main/resources/pipeline-template-view-rules

# For templates, in pbsmrtpipe repo:
#   pbsmrtpipe show-templates --output-templates-json out-dir
# and copy *fal* to
# //depot/software/smrtanalysis/services-ui/scala/services/secondary-smrt-server/src/main/resources/resolved-pipeline-templates
#
# cd ~/repo/pb/smrtanalysis-client/smrtanalysis/services-ui/scala/services/secondary-smrt-server/src/main/resources/resolved-pipeline-templates

.PHONY: all modules runners update utest
