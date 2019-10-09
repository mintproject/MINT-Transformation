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
        Topoflow4SoilWriteFunc.I.output_dir: "/ws/examples/scotts_transformations/tmp/soil_",
        Topoflow4SoilWriteFunc.I.layer: "1",
        Topoflow4SoilWriteFunc.I.input_dir: "/ws/scott_oct_eval/soilGrids/",
        Topoflow4SoilWriteFunc.I.DEM_bounds: "34.221249999999, 7.362083333332, 36.446249999999, 9.503749999999",
        Topoflow4SoilWriteFunc.I.DEM_ncols: "267",
        Topoflow4SoilWriteFunc.I.DEM_nrows: "257",


        # Topoflow4SoilWriteFunc.I.input_dir: "/ws/examples/scotts_transformations/soil",
        # Topoflow4SoilWriteFunc.I.DEM_bounds: "24.079583333333, 6.565416666666, 27.379583333333, 10.132083333333",
        # Topoflow4SoilWriteFunc.I.DEM_ncols: "396",
        # Topoflow4SoilWriteFunc.I.DEM_nrows: "428",
        Topoflow4SoilWriteFunc.I.DEM_xres_arcsecs: "30",
        Topoflow4SoilWriteFunc.I.DEM_yres_arcsecs: "30",
    }

    outputs = pipeline.exec(inputs)
