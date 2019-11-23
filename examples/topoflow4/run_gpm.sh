#!/usr/bin/env bash

set -e

source ./locations.sh

function exec {
    name=$1
    year=$2
    resolution=$3
    bbox="${AREAS[$name]}"

    output_dir=/data/mint/topoflow/$name/gpm/$year\_$resolution
    echo "Going to run and output to $output_dir"

    if [[ ! -d "$output_dir" ]]; then
        mkdir -p "$output_dir"
    fi

    if [[ ! -d "$output_dir/cropped_region" ]]; then
        mkdir -p "$output_dir/cropped_region"
    fi

    python -m dtran.main exec_pipeline \
        --config ./topoflow_climate.yml \
        --tf_climate.input_dir=/data/mint/gpm/$year \
        --tf_climate.crop_region_dir=$output_dir/cropped_region \
        --tf_climate.output_file=$output_dir/climate.rts \
        --tf_climate.DEM_bounds="$bbox" \
        --tf_climate.DEM_xres_arcsecs=$resolution \
        --tf_climate.DEM_yres_arcsecs=$resolution > $output_dir/run.log

    python -m dtran.main exec_pipeline \
        --config ./topoflow_climate_per_month.yml \
        --tf_climate_month.grid_dir=$output_dir/cropped_region \
        --tf_climate_month.output_file=$output_dir/climate.rts > $output_dir/run_climate.log

    pushd $output_dir
    # compress the file so we can delete the original file, which is much bigger
    tar -czf data.tar.gz run.log run_climate.log climate.rts climate.rti climate.*.rts
    # remove the uncompressed files
    rm climate.*
    popd
}

#year=$1
years="2010 2011 2012 2013 2014 2015 2016 2017 2018"

for year in $years; do
    bash ./download_gpm.sh $year /data/mint/gpm

    for name in "${!AREAS[@]}"; do
        exec $name $year 30
        exec $name $year 60
    done

    # remove the data
    rm -r /data/mint/gpm/$year

    # upload the data
    bash ./upload_gpm.sh $year
done
