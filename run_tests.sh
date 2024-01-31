#!/bin/sh
cd ~
~/.local/bin/coverage erase
rm -rf htmlcov
~/.local/bin/coverage run --branch --source=/home/cdsw/cprices/cprices/steps/ -m unittest discover -s /home/cdsw/cprices/tests/
~/.local/bin/coverage report -m
coverage html
