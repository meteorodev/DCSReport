#!/bin/bash
#this script make venv base venv package from python 3
# install dependecies
HOY=$(date +%d-%m-%Y)
dir_str=`dirname $0`
cd $dir_str
logFile=$dir_str"/"$HOY"_app.log"
#echo "$logFile"
./venv/bin/python ./main.py >> $logFile
