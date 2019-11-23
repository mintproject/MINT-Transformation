#!/usr/bin/env bash
set -e

source ./locations.sh

function upload_file {
    local_file=$1
    remote_file=$2
    echo "Upload file to /remote.php/webdav/$remote_file"
    curl -X PUT -u datacatalog:$PASSWORD \
        https://files.mint.isi.edu/remote.php/webdav/$remote_file --upload-file $local_file --progress-bar | tee /dev/null
}

#years=(2008)
years=($1)
resolutions=(30 60)

for name in "${!AREAS[@]}"; do
    for year in $years; do
        for res in $resolutions; do
            upload_file /data/mint/topoflow/$name/gpm/$year\_$res/data.tar.gz topoflow/$name\_gpm_$year\_$res.tar.gz
        done
    done
done