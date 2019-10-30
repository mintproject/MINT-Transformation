import os
from pathlib import Path

from dtran import Pipeline
from funcs import Topoflow4SoilWriteFunc
from eval_soil_climate_defs import BARO, LOL_KURU

if __name__ == "__main__":
    import sys
    area_str = sys.argv[1]
    layer_str = sys.argv[2]
    if area_str == "baro_res30":
        area = BARO["res30"]
    elif area_str == "baro_res60":
        area = BARO["res60"]
    elif area_str == "kuru_res30":
        area = LOL_KURU["res30"]
    elif area_str == "kuru_res60":
        area = LOL_KURU["res60"]

    pipeline = Pipeline(
        [Topoflow4SoilWriteFunc],
        wired=[ ],
    )

    inputs = {
        Topoflow4SoilWriteFunc.I.layer: layer_str,
        Topoflow4SoilWriteFunc.I.input_dir: "/ws/oct_eval_data/soilGrids/",
        Topoflow4SoilWriteFunc.I.output_dir: f"/ws/examples/scotts_transformations/tmp/soil_{area_str}_l{layer_str}",
        Topoflow4SoilWriteFunc.I.DEM_bounds: area['bbox'],
        Topoflow4SoilWriteFunc.I.DEM_ncols: area['ncols'],
        Topoflow4SoilWriteFunc.I.DEM_nrows: area['nrows'],
        Topoflow4SoilWriteFunc.I.DEM_xres_arcsecs: area['xres'],
        Topoflow4SoilWriteFunc.I.DEM_yres_arcsecs: area['yres'],
    }

    outputs = pipeline.exec(inputs)
