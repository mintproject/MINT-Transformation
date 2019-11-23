#!/usr/bin/env bash

set -e

source ./locations.sh

for c in "${!AREAS[@]}"; do
    printf "%s is in %s\n" "$c" "${AREAS[$c]}"
done