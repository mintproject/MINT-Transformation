#!/bin/bash
set -e

cd /ws
source /root/.bashrc
dotenv -f .env.docker run python -m dtran.main exec_pipeline --config "$@"