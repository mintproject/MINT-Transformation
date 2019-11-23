#!/usr/bin/env bash
set -e

source ./locations.sh

function run_transformation {
    area=$1
    bbox=$2
    layers="1 2 3 4 5 6 7"

    for layer in $layers
    do
        echo "Run transformation on $area and layer $layer"
        python dtran.main exec_pipeline \
            --config ./topoflow_soil.yml \
            --tf_soil.output_dir=/data/mint/topoflow/$area/soilGrids \
            --tf_soil.DEM_bounds=$bbox \
            --tf_soil.layer=$layer
    done
}

for name in "${!AREAS[@]}"; do
    bbox=${AREAS[$name]}
    run_transformation $name "$bbox"
done
