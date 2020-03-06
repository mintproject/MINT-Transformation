from eval_soil_climate_defs import BARO, LOL_KURU

from dtran import Pipeline
from funcs import Topoflow4ClimateWriteFunc

if __name__ == "__main__":
    import sys
    area_str = sys.argv[1]
    area = BARO if area_str == "baro" else LOL_KURU

    pipeline = Pipeline(
        [Topoflow4ClimateWriteFunc],
        wired=[ ],
    )

    inputs = {
        Topoflow4ClimateWriteFunc.I.input_dir: "/data/mint/gpm",
        Topoflow4ClimateWriteFunc.I.temp_dir: f"/data/mint/gpm_grid_{area_str}",
        Topoflow4ClimateWriteFunc.I.output_file: f"/data/mint/{area_str}/climate_all.rts",
        Topoflow4ClimateWriteFunc.I.DEM_bounds: area['bbox'],
        Topoflow4ClimateWriteFunc.I.DEM_ncols: area['ncols'],
        Topoflow4ClimateWriteFunc.I.DEM_nrows: area['nrows'],
        Topoflow4ClimateWriteFunc.I.DEM_xres_arcsecs: area['xres'],
        Topoflow4ClimateWriteFunc.I.DEM_yres_arcsecs: area['yres'],
    }

    outputs = pipeline.exec(inputs)