#!/usr/bin/env bash
set -e

YEAR=$1
OUTPUT_DIR=$2

cd $OUTPUT_DIR
wget --user datacatalog --password $PASSWORD https://files.mint.isi.edu/remote.php/webdav/gpm_$YEAR.tar.gz
tar -xf gpm_$YEAR.tar.gz
rm gpm_$YEAR.tar.gz
mv gpm_$YEAR $YEAR
