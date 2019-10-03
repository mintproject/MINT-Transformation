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
        Topoflow4SoilWriteFunc.I.input_dir: "./examples/scotts_transformations/soil/",
        Topoflow4SoilWriteFunc.I.output_dir: "./examples/scotts_transformations/soil/test_",
        Topoflow4SoilWriteFunc.I.layer: "1",
        Topoflow4SoilWriteFunc.I.DEM_bounds: "24.079583333333, 6.565416666666, 27.379583333333, 10.132083333333",
        Topoflow4SoilWriteFunc.I.DEM_ncols: "396",
        Topoflow4SoilWriteFunc.I.DEM_nrows: "428",
        Topoflow4SoilWriteFunc.I.DEM_xres: "0.008333",
        Topoflow4SoilWriteFunc.I.DEM_yres: "0.008333",
    }

    outputs = pipeline.exec(inputs)