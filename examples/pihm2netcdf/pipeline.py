import json
from datetime import datetime
import os
from pathlib import Path

from dtran import Pipeline
from examples.pihm2cycles.cycle_write_func import CyclesWriteFunc
from examples.pihm2cycles.pihm2cycles_func import Pihm2CyclesFunc
from examples.pihm2netcdf.cell2point_func import Cell2PointFunc
from examples.pihm2netcdf.mint_netcdf_write_func import MintNetCDFWriteFunc
from examples.pihm2netcdf.pihm_monthly_flooding_func import PihmMonthlyFloodingFunc
from funcs import ReadFunc
from funcs.merge_func import MergeFunc

if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent / "resources"

    pipeline = Pipeline(
        [ReadFunc, Cell2PointFunc, PihmMonthlyFloodingFunc, MintNetCDFWriteFunc],
        wired=[
            ReadFunc.O.data == PihmMonthlyFloodingFunc.I.graph,
            PihmMonthlyFloodingFunc.O.data == MintNetCDFWriteFunc.I.data,
        ],
    )

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "points.model.yml",
        ReadFunc.I._1.resources: json.dumps({"surf": str(wdir / "surf.csv"), "points": str(wdir / "surf_points.csv")}),
        Cell2PointFunc.I.cell2point_file: wdir / "cell2points.R",
        Cell2PointFunc.I.cell_file: wdir / "GISlayer" / "Cells.shp",
        Cell2PointFunc.I.point_file: wdir / "surf_points.csv",
        PihmMonthlyFloodingFunc.I.mean_space: 0.05,
        PihmMonthlyFloodingFunc.I.start_time: datetime.strptime("2017-01-01 00:00:00", '%Y-%m-%d %H:%M:%S'),
        PihmMonthlyFloodingFunc.I.end_time: datetime.strptime("2017-12-30 00:00:00", '%Y-%m-%d %H:%M:%S'),
        PihmMonthlyFloodingFunc.I.threshold: 0.05,
        MintNetCDFWriteFunc.I.output_file: "MONTHLY_GRIDDED_SURFACE_INUNDATION_2017.nc"
    }

    outputs = pipeline.exec(inputs)
