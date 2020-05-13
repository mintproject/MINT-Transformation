#!/usr/bin/env bash
set -e

HOME_DIR="/ws/examples/topoflow4"

source "$HOME_DIR/locations.sh"

function run_transformation {
    area=$1
    bbox=$2
    layers="1 2 3 4 5 6 7"

    if [[ ! -d "/tmp/demo/$area" ]]; then
        mkdir -p "/tmp/demo/$area"
    fi

    for layer in $layers
    do
        echo "Run transformation on $area and layer $layer"
        dotenv run python -m dtran.main exec_pipeline \
            --config "$HOME_DIR/topoflow_soil.yml" \
            --tf_soil.output_dir="/tmp/demo/$area" \
            --tf_soil.DEM_bounds="$bbox" \
            --tf_soil.layer=$layer > /tmp/demo/$area/run.log
    done
}

cd /ws

for name in "${!AREAS[@]}"; do
     bbox=${AREAS[$name]}
     run_transformation $name "$bbox"
done
