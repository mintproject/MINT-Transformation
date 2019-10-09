import os
from pathlib import Path

from dtran import Pipeline
from funcs import Topoflow4ClimateWriteFunc

if __name__ == "__main__":
   

    pipeline = Pipeline(
        [Topoflow4ClimateWriteFunc],
        wired=[ ],
    )

    inputs = {
        Topoflow4ClimateWriteFunc.I.input_dir: "./oct_eval_data/gpm_2014_01_01",
        Topoflow4ClimateWriteFunc.I.output_file: "./oct_eval_data/gpm_2014_01_01//test_scott.rts",
        Topoflow4ClimateWriteFunc.I.DEM_bounds: "7.362083333332, 9.503749999999, 34.221249999999, 36.446249999999",
        Topoflow4ClimateWriteFunc.I.DEM_ncols: "267",
        Topoflow4ClimateWriteFunc.I.DEM_nrows: "257",
        Topoflow4ClimateWriteFunc.I.DEM_xres_arcsecs: "30",
        Topoflow4ClimateWriteFunc.I.DEM_yres_arcsecs: "30",
    }

    outputs = pipeline.exec(inputs)