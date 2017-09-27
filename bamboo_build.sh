#!/bin/bash
source bamboo_setup.sh

set -vex
echo 'hi'
ls -larth ..
find ../tag_deps
WHEELHOUSE=$(pwd)/../tag_deps/gcc-6.4.0/wheelhouse

pip install --user --no-index --find-links=${WHEELHOUSE} pbcommand pbreports pbcoretools

pushd ../pypeFLOW
pip install --user --edit .
popd

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
