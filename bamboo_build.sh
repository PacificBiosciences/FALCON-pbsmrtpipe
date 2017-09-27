#!/bin/bash
source bamboo_setup.sh

set -vex
echo 'hi'

pip install --user nose
pip install --user pytest

pushd ../pbcommand
pip install --user --edit .
popd

pushd ../pbreports
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

make utest
