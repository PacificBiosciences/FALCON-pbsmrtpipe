#!/bin/bash
source bamboo_setup.sh

set -vex
echo 'hi'
ls -larth ..
find ../tag_deps

pushd ../pbcommand
pip install --user --edit .
popd

pushd ../pbreports
pip install --user --edit .
popd

pushd ../pbcoretools
pip install --user --edit .
popd

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
