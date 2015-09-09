all:
	pip install -v -e .
	pb-falcon -h
run-dev:
	cd dev_01; pbsmrtpipe  pipeline $$(pwd)/workflow_id.xml --debug  -e e_01:$$(pwd)/input.txt --preset-xml=$$(pwd)/preset.xml    --output-dir=$$(pwd)/job_output
run:
	cd falcon; pbsmrtpipe  pipeline $$(pwd)/workflow_id.xml --debug  -e e_01:$$(pwd)/input.txt --preset-xml=$$(pwd)/preset.xml    --output-dir=$$(pwd)/job_output
regen:
	python -m pbfalcon.tasks.scatter0_run_daligner_jobs --emit-tool-contract >| ../pbsmrtpipe/pbsmrtpipe/registered_tool_contracts/pbfalcon.tasks.scatter0_run_daligner_jobs_tool_contract.json
	python -m pbfalcon.tasks.scatter1_run_daligner_jobs --emit-tool-contract >| ../pbsmrtpipe/pbsmrtpipe/registered_tool_contracts/pbfalcon.tasks.scatter1_run_daligner_jobs_tool_contract.json
	python -m pbfalcon.tasks.gather0_run_daligner_jobs --emit-tool-contract >| ../pbsmrtpipe/pbsmrtpipe/registered_tool_contracts/pbfalcon.tasks.gather0_run_daligner_jobs_tool_contract.json
	python -m pbfalcon.tasks.gather1_run_daligner_jobs --emit-tool-contract >| ../pbsmrtpipe/pbsmrtpipe/registered_tool_contracts/pbfalcon.tasks.gather1_run_daligner_jobs_tool_contract.json
other:
	cd ../pbsmrtpipe/pbsmrtpipe/registered_tool_contracts && python -m pbfalcon.tasks.basic emit-tool-contracts
