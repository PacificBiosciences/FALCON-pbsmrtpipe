all:
	pip install -v -e .
	pb-falcon -h
run-dev:
	cd dev_01; pbsmrtpipe  pipeline $$(pwd)/workflow_id.xml --debug  -e e_01:$$(pwd)/input.txt --preset-xml=$$(pwd)/preset.xml    --output-dir=$$(pwd)/job_output
run:
	cd falcon; pbsmrtpipe  pipeline $$(pwd)/workflow_id.xml --debug  -e e_01:$$(pwd)/input.txt --preset-xml=$$(pwd)/preset.xml    --output-dir=$$(pwd)/job_output

PBFALCON_TC_RUNNERS:= \
	pbfalcon.cli.gen_config \
	pbfalcon.tasks.scatter0_run_daligner_jobs \
	pbfalcon.tasks.scatter1_run_daligner_jobs \
	pbfalcon.tasks.gather0_run_daligner_jobs \
	pbfalcon.tasks.gather1_run_daligner_jobs \

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

# For templates, in pbsmrtpipe repo:
#   pbsmrtpipe show-templates --output-templates-json out-dir
# and copy *fal* to
# //depot/software/smrtanalysis/services-ui/scala/services/secondary-smrt-server/src/main/resources/pipeline-template-view-rules

.PHONY: all modules runners update
