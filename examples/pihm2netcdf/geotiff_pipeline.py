import json
import os
from datetime import datetime
from pathlib import Path

from dtran import Pipeline
from examples.pihm2netcdf.cell2point_func import Cell2PointFunc
from examples.pihm2netcdf.mint_geotifff_write_func import MintGeoTiffWriteFunc
from examples.pihm2netcdf.pihm_monthly_average_flooding_func import PihmMonthlyAverageFloodingFunc
from funcs import ReadFunc

if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent / "resources"

    pipeline = Pipeline(
        [ReadFunc, Cell2PointFunc, PihmMonthlyAverageFloodingFunc, MintGeoTiffWriteFunc],
        wired=[
            ReadFunc.O.data == PihmMonthlyAverageFloodingFunc.I.graph,
            PihmMonthlyAverageFloodingFunc.O.data == MintGeoTiffWriteFunc.I.data,
        ],
    )

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "points.model.yml",
        ReadFunc.I._1.resources: json.dumps(
            {"surf": str(wdir / "surf.csv"), "points": str(wdir / "surf_points.csv")}
        ),
        Cell2PointFunc.I.cell2point_file: wdir / "cell2points.R",
        Cell2PointFunc.I.cell_file: wdir / "GISlayer" / "Cells.shp",
        Cell2PointFunc.I.point_file: wdir / "surf_points.csv",
        PihmMonthlyAverageFloodingFunc.I.mean_space: 0.05,
        PihmMonthlyAverageFloodingFunc.I.start_time: datetime.strptime(
            "2017-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        ),
        PihmMonthlyAverageFloodingFunc.I.end_time: datetime.strptime(
            "2017-12-31 00:00:00", "%Y-%m-%d %H:%M:%S"
        ),
        # MintGeoTiffWriteFunc.I.output_file: wdir / "MONTHLY_GRIDDED_SURFACE_INUNDATION_2017.tif",
        MintGeoTiffWriteFunc.I.output_file: wdir / "MONTHLY_GRIDDED_SURFACE_INUNDATION_2017",
        MintGeoTiffWriteFunc.I.is_multiple_files: True,
    }

    outputs = pipeline.exec(inputs)
