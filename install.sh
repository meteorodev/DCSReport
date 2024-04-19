#!/bin/bash
#this script make venv base venv package from python 3
# install dependecies
dir_str=`dirname $0`
cd $dir_str
echo "$(pwd)"
python3 -m venv ./venv
source ./venv/bin/activate
pip install -r requierements_2.txt

