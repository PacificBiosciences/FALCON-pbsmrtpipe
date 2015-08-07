all:
	pip install -v .
	pb-falcon -h
run:
	cd dev_01; pbsmrtpipe  pipeline $$(pwd)/workflow_id.xml --debug  -e e_01:$$(pwd)/input.txt --preset-xml=$$(pwd)/preset.xml    --output-dir=$$(pwd)/job_output
