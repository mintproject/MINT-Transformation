#!/bin/bash

PYTHONPATH=$(pwd) python examples/scotts_transformations/pipeline_climate.py baro_res60
PYTHONPATH=$(pwd) python examples/scotts_transformations/pipeline_climate_per_month.py baro_res60
PYTHONPATH=$(pwd) python examples/scotts_transformations/pipeline_climate.py kuru_res60
PYTHONPATH=$(pwd) python examples/scotts_transformations/pipeline_climate_per_month.py kuru_res60