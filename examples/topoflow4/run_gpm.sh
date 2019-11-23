#!/usr/bin/env bash

set -e

source ./locations.sh

function exec {
    name=$1
    year=$2
    resolution=$3
    bbox="${AREAS[$name]}"

    bash ./download_gpm.sh $year /data/mint/gpm

    output_dir=/data/mint/topoflow/$name/gpm/$year\_$resolution
    echo "Going to run and output to $output_dir"

    if [[ ! -d "$output_dir" ]]; then
        mkdir -p "$output_dir"
    fi

    if [[ ! -d "$output_dir/cropped_region" ]]; then
        mkdir -p "$output_dir/cropped_region"
    fi

#    python -m dtran.main exec_pipeline \
#        --config ./topoflow_climate.yml \
#        --tf_climate.input_dir=/data/mint/gpm/$year \
#        --tf_climate.crop_region_dir=$output_dir/cropped_region \
#        --tf_climate.output_file=$output_dir/climate.rts \
#        --tf_climate.DEM_bounds="$bbox" \
#        --tf_climate.DEM_xres_arcsecs=$resolution \
#        --tf_climate.DEM_yres_arcsecs=$resolution > $output_dir/run.log
}

year=$1

for name in "${!AREAS[@]}"; do
    exec $name $year 30
    exec $name $year 60
done