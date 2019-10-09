import os
from pathlib import Path

from dtran import Pipeline
from funcs import Topoflow4SoilWriteFunc

if __name__ == "__main__":
   

    pipeline = Pipeline(
        [Topoflow4SoilWriteFunc],
        wired=[ ],
    )

    inputs = {
        Topoflow4SoilWriteFunc.I.input_dir: "./oct_eval_data/soilGrids/",
        Topoflow4SoilWriteFunc.I.output_dir: "./oct_eval_data/soilGrids/test_",
        Topoflow4SoilWriteFunc.I.layer: "1",
        Topoflow4SoilWriteFunc.I.DEM_bounds: "7.362083333332, 9.503749999999, 34.221249999999, 36.446249999999",
        Topoflow4SoilWriteFunc.I.DEM_ncols: "267",
        Topoflow4SoilWriteFunc.I.DEM_nrows: "257",
        Topoflow4SoilWriteFunc.I.DEM_xres_arcsecs: "30",
        Topoflow4SoilWriteFunc.I.DEM_yres_arcsecs: "30",
    }

    outputs = pipeline.exec(inputs)
