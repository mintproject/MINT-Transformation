#!/bin/bash
docker ps -a -q | xargs docker rm -v
rm /tmp/mintdt/*
