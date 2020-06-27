#!/bin/bash

# This bash scripts run the transformation to produce all GLDAS datasets. Should run it with dotenv

if [[ -z "${HOME_DIR}" ]]; then
    echo "Home directory is not defined. Exit"
    exit -1
fi

# change the working directory to the current project
cd ${HOME_DIR}

# define parameters
awash="37.829583333333, 6.654583333333, 39.934583333333, 9.374583333333"
baro="34.221249999999, 7.362083333332, 36.450416666666, 9.503749999999"
shebelle="38.159583333333, 6.319583333333, 43.559583333333, 9.899583333333"
ganale="39.174583333333, 5.527916666666, 41.124583333333, 7.098749999999"
guder="37.149583333333, 8.596250000000, 38.266250000000, 9.904583333333"
muger="37.807916666667, 8.929583333333, 39.032916666667, 10.112916666667"
beko="35.241249999999, 6.967916666666, 35.992916666666, 7.502916666666"
alwero="34.206249999999, 7.415416666666, 35.249583333333, 8.143749999999"
AREAS=("$awash" "$baro" "$shebelle" "$ganale" "$guder" "$muger" "$beko" "$alwero")
AREA_NAMES=("awash" "baro" "shebelle" "ganale" "guder" "muger" "beko" "alwero")

# START_TIMES
# end_year=2019

ARCSECS=(30 60)

# create a basic command
CONFIG_FILE=$HOME_DIR/examples/topoflow4/dev/tf_climate.gldas_remote.yml
RUN_DIR=$HOME_DIR/data/tf_gldas
CMD="python -m dtran.main exec_pipeline --config $CONFIG_FILE"

for ((i=0;i<${#AREAS[@]};++i)); do
    area=${AREAS[i]}
    area_name=${AREA_NAMES[i]}
    
    for arcsec in "${ARCSECS}"; do
        year=2008
        echo --GLDAS.start_time $year-01-01T00:00:00 \
            --GLDAS.end_time $year-01-01T00:00:00 \
            --geotiff_writer.output_dir $RUN_DIR/$year/geotiff \
            --topoflow.cropped_geotiff_dir $RUN_DIR/$year/${area_name}_geotiff \
            --topoflow.xres_arcsecs $arcsec \
            --topoflow.xres_arcsecs $arcsec \
            --topoflow.bounds "\"$area\""
    done
done
