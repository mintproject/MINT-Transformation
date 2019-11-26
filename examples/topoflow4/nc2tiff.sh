#!/usr/bin/env bash
set -e

source ./locations.sh

function exec {
    year=$1

    # prepare input dir
    bash ./download_gpm.sh $year /data/mint/gpm

    # create output dir
    if [[ ! -d "/data/mint/gpm_tiff/$year" ]]; then
        mkdir -p "/data/mint/gpm_tiff/$year"
    fi

    python -m dtran.main exec_pipeline \
        --config ./nc2tiff.yml \
        --nc2tiff.input_dir=/data/mint/gpm/$year \
        --nc2tiff.output_dir=/data/mint/gpm_tiff/$year
}

exec 2008
#years=(2008 2009 2010 2011 2012 2013 2014 2015 2016 2017)
#
#for year in $years; do
#done