import os, numpy as np
from pathlib import Path

from dtran import Pipeline
from funcs import Topoflow4ClimateWriteFunc

if __name__ == "__main__":
    pipeline = Pipeline(
        [Topoflow4ClimateWriteFunc],
        wired=[ ],
    )

    area = "alwero"
    year = "2008"
    res = 30
    bbox = "34.206249999999, 7.415416666666, 35.249583333333, 8.143749999999"

    inputs = {
        Topoflow4ClimateWriteFunc.I.input_dir: f"/data/mint/gldas/{year}",
        Topoflow4ClimateWriteFunc.I.crop_region_dir: f"/data/mint/topoflow/{area}/gldas/{year}_{res}/cropped_region",
        Topoflow4ClimateWriteFunc.I.output_file: f"/data/mint/topoflow/{area}/gldas/{year}_{res}/climate.rts",
        Topoflow4ClimateWriteFunc.I.var_name: "Rainf_f_tavg",
        Topoflow4ClimateWriteFunc.I.DEM_bounds: bbox,
        Topoflow4ClimateWriteFunc.I.DEM_xres_arcsecs: str(res),
        Topoflow4ClimateWriteFunc.I.DEM_yres_arcsecs: str(res),
    }

    outputs = pipeline.exec(inputs)