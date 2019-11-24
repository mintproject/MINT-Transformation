#!/usr/bin/env bash
set -e
YEAR=$1
OUTPUT_DIR=$2
function convert { date -d "$1-01-01 +$2 days -1 day" "+%Y%m%d"; }

cd $OUTPUT_DIR
if [[ ! -d "$OUTPUT_DIR/$YEAR" ]]; then
    echo "Download fldas files"
    ENDDATE="$(convert $(($YEAR+1)) 1)"
    for i in {1..366}; do
        DATE="$(convert $YEAR $i)"
        if [[ $DATE <  $ENDDATE ]]; then
            MONTH=${DATE:4:2}
            wget "https://data.mint.isi.edu/files/raw-data/FLDAS/FLDAS_NOAH01_A_EA_D.001/$YEAR/$MONTH/FLDAS_NOAH01_A_EA_D.A$DATE.001.nc" || :
        fi
    done
else
    echo "Skip download because the files exist"
fi