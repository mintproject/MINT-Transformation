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
        Topoflow4ClimateWriteFunc.I.input_dir: "./examples/scotts_transformations/rivertools/12_NC_Files",
        Topoflow4ClimateWriteFunc.I.output_file: "./examples/scotts_transformations/rivertools/12_NC_Files/test_scott.rts",
        Topoflow4ClimateWriteFunc.I.DEM_bounds: "24.079583333333, 6.565416666666, 27.379583333333, 10.132083333333",
        Topoflow4ClimateWriteFunc.I.DEM_ncols: "396",
        Topoflow4ClimateWriteFunc.I.DEM_nrows: "428",
        Topoflow4ClimateWriteFunc.I.DEM_xres: "0.008333",
        Topoflow4ClimateWriteFunc.I.DEM_yres: "0.008333",
    }

    outputs = pipeline.exec(inputs)