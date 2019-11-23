#!/usr/bin/env bash
set -e

YEAR=$1
OUTPUT_DIR=$2

wget --user datacatalog --password $PASSWORD https://files.mint.isi.edu/remote.php/webdav/gpm_$YEAR.tar.gz