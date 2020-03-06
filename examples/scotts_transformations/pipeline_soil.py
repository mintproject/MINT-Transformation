from eval_soil_climate_defs import BARO, LOL_KURU

from dtran import Pipeline
from funcs import Topoflow4SoilWriteFunc

if __name__ == "__main__":
    import sys
    area_str = sys.argv[1]
    layer_str = sys.argv[2]
    area = BARO if area_str == "baro" else LOL_KURU
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
