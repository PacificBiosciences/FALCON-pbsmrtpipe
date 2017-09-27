#!/bin/bash
source bamboo_setup.sh

set -vex
echo 'hi'
WHEELHOUSE=$(pwd)/../wheelhouse

ls -larth ..
find ../tag_deps
rsync -va ../tag_deps/gcc-6.4.0/wheelhouse/ ${WHEELHOUSE}
rsync -va ../pypeflow_wheel/gcc-6.4.0/wheelhouse/ ${WHEELHOUSE}

pip install --user --no-index --find-links=${WHEELHOUSE} pbcommand pbreports pbcoretools pypeflow

#pushd ../pypeFLOW
#pip install --user --edit .
#popd

pushd ../FALCON
pip install --user --edit .
popd

pushd ../FALCON-polish
pip install --user --edit .
popd

pip install --user --edit .

pip install --user nose
pip install --user pytest
pip install --user pylint

make utest
make pylint
