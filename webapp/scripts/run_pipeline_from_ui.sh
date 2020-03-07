#!/bin/bash
# set -e

SESSION_ID=$1

cp "/tmp/config.${SESSION_ID}.yml" /run/config.yml
cp "/tmp/run.start.${SESSION_ID}.log" /run/start.run.log
cp "/tmp/run.end.${SESSION_ID}.log" /run/end.run.log

cd /ws
PYTHONPATH=$(pwd):$(pwd)/extra_libs:$PYTHONPATH dotenv -f .env.docker run python -m dtran.main exec_pipeline --config /run/config.yml
TZ=":America/Los_Angeles" date +'%Y-%m-%dT%H:%M:%S' >> /run/end.run.log