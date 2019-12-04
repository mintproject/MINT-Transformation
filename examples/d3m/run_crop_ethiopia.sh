#!/usr/bin/env bash

set -e
years="2008"

for year in $years; do
    if [[ ! -d "/data/mint/gpm_ethiopia/$year" ]]; then
        mkdir -p "/data/mint/gpm_ethiopia/$year"
    fi

    # crop the data
    python crop_ethiopia.py /data/mint/gpm/$year /data/mint/gpm_ethiopia/$year
done