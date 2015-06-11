#!/bin/bash
#
# Helper script to LOCALLY test opengever.maintenance with all test-*.cfgs

PYTHON="python2.7"

for CFG in test-og-*
do
    echo "Testing against ${CFG}..."
    rm -f ./buildout.cfg
    ln -s ${CFG} buildout.cfg
    ${PYTHON} bootstrap.py
    bin/buildout
    bin/test
    echo
done
