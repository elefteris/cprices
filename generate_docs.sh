#!/bin/sh

ROOTDIR=/home/cdsw/cpdt_core

rm -rf $ROOTDIR/docs/_build/
mkdir $ROOTDIR/docs/_build/

#rm -rf $ROOTDIR/docs/html/*
rm -rf $ROOTDIR/docs/markdown/*

cp $ROOTDIR/docs/*.rst $ROOTDIR/docs/_build/
cp $ROOTDIR/docs/conf.py $ROOTDIR/docs/_build/conf.py

/home/cdsw/.local/bin/sphinx-apidoc -f -o $ROOTDIR/docs/_build/ $ROOTDIR
#/home/cdsw/.local/bin/sphinx-build -b html $ROOTDIR/docs/_build $ROOTDIR/docs/html/
/home/cdsw/.local/bin/sphinx-build -M markdown $ROOTDIR/docs/_build $ROOTDIR/docs/markdown
