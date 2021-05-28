#!/bin/bash
set -e
#workaround change directory hack
full_config_path=$(realpath "$@"))
cd /ws
source /root/.bashrc
dotenv -f .env.docker run python -m dtran.main exec_pipeline --config ${full_config_path}
