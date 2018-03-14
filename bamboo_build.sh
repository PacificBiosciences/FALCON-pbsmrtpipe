#!/bin/bash -e
#source /mnt/software/Modules/current/init/bash
type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module load python/2-UCS4

set -vex
which python
which pip

mkdir -p LOCAL
export PYTHONUSERBASE=$(pwd)/LOCAL
export PATH=${PYTHONUSERBASE}/bin:${PATH}
WHEELHOUSE=$(pwd)/../wheelhouse

ls -larth ..
find ../tag_deps
rsync -va ../tag_deps/gcc-6.4.0/wheelhouse/ ${WHEELHOUSE}
rsync -va ../pypeflow_wheel/gcc-6.4.0/wheelhouse/ ${WHEELHOUSE}
rsync -va ../falcon_wheel/gcc-6.4.0/wheelhouse/ ${WHEELHOUSE}

pip install --user --no-index --find-links=${WHEELHOUSE} pbcommand pbreports pbcoretools pypeflow falcon_kit

python -c 'import pypeflow; print pypeflow'
python -c 'import falcon_kit; print falcon_kit'

pushd ../FALCON-polish
pip install --user --edit .
popd

pip install --user --edit .

# For speed, use cdunn wheelhouse.
WHEELHOUSE=/home/UNIXHOME/cdunn/wheelhouse/gcc-6/
pip install --user --no-index --find-links=${WHEELHOUSE} pip pytest pytest-cov pylint nose

export MY_TEST_FLAGS="-v -s --durations=0 --cov=. --cov-report=term-missing --cov-report=xml:coverage.xml --cov-branch"
make pytest
#sed -i -e 's@filename="@filename="./pbfalcon/@g' coverage.xml

make pylint

pwd
ls -larth
find . -name '*.pyc' | xargs rm
