#!/bin/bash
docker ps -a -q --filter ancestor=mint_dt| xargs docker rm -v
rm /tmp/mintdt/*
rm /tmp/*.yml
rm /tmp/run.*.log