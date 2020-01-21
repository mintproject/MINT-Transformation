#!/bin/bash
docker ps -a -q --filter ancestor=mint_dt | xargs docker rm -v
rm /tmp/mintdt/*
