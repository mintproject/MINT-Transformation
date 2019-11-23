#!/usr/bin/env bash

set -e

function upload_file {
    local_file=$1
    remote_file=$2
    curl -X PUT -u datacatalog:$PASSWORD \
        https://files.mint.isi.edu/remote.php/webdav/$remote_file --upload-file $local_file --progress-bar | tee /dev/null
}

for name in "${!AREAS[@]}"; do
    pushd /data/mint/topoflow/$name
    tar -czf isric_soil.tar.gz isric_soil
    popd

    upload_file /data/mint/topoflow/$name/isric_soil.tar.gz topoflow/$name_isric_soil.tar.gz
done