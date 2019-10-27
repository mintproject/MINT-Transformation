import os
from pathlib import Path

from dtran import Pipeline
from funcs import Topoflow4ClimateWritePerMonthFunc
from eval_soil_climate_defs import BARO, LOL_KURU

if __name__ == "__main__":
    import sys
    area_str = sys.argv[1]
    area = BARO if area_str == "baro" else LOL_KURU

    pipeline = Pipeline(
        [Topoflow4ClimateWritePerMonthFunc],
        wired=[ ],
    )

    inputs = {
        Topoflow4ClimateWritePerMonthFunc.I.grid_dir: f"/data/mint/gpm_grid_{area_str}",
        Topoflow4ClimateWritePerMonthFunc.I.date_regex: '3B-HHR-E.MS.MRG.3IMERG.(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})',
        Topoflow4ClimateWritePerMonthFunc.I.output_file: f"/data/mint/climate_{area_str}.rts",
    }

    outputs = pipeline.exec(inputs)