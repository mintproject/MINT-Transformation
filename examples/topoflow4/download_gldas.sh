#!/usr/bin/env bash
set -e

YEAR=$1
OUTPUT_DIR=$2
function convert { date -d "$1-01-01 +$2 days -1 day" "+%Y%m%d"; }

cd $OUTPUT_DIR
if [[ ! -d "$OUTPUT_DIR/$YEAR" ]]; then
    echo "Download gldas files"
    ENDDATE="$(convert $(($YEAR+1)) 1)"
    for i in {1..366}; do
        DATE="$(convert $YEAR $i)"
        if [[ $DATE <  $ENDDATE ]]; then
            for j in 0000 0300 0600 0900 1200 1500 1800 2100; do
                wget "https://data.mint.isi.edu/files/raw-data/GLDAS/$YEAR/$(printf "%03d" $i)/GLDAS_NOAH025_3H.A$DATE.$j.021.nc4" || :
            done
        fi
    done
else
    echo "Skip download because the files exist"
fi