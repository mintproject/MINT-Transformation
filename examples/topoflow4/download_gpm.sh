#!/usr/bin/env bash
set -e

YEAR=$1
OUTPUT_DIR=$2

cd $OUTPUT_DIR

if [[ ! -d "$OUTPUT_DIR/$YEAR" ]]; then
    echo "Download and uncompress gpm files $YEAR"

    wget --user datacatalog --password $PASSWORD https://files.mint.isi.edu/remote.php/webdav/gpm_$YEAR.tar.gz
    tar -xf gpm_$YEAR.tar.gz
    rm gpm_$YEAR.tar.gz
    mv gpm* $YEAR
else
    echo "Skip download because the files exist"
fi


