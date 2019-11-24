#!/usr/bin/env bash

set -e

source ./locations.sh

name=$1
year=$2
resolution=$3
bbox="${AREAS[$name]}"

output_dir=/data/mint/topoflow/$name/gldas/$year\_$resolution

python -m dtran.main exec_pipeline \
    --config ./topoflow_climate_per_month.yml \
    --tf_climate_month.grid_dir=$output_dir/cropped_region \
    --tf_climate_month.date_regex="GLDAS_NOAH025_3H.A(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})" \
    --tf_climate_month.output_file=$output_dir/climate.rts