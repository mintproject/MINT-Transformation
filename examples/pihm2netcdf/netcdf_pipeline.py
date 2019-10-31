import json
import os
from datetime import datetime
from pathlib import Path

from dtran import Pipeline
from examples.pihm2netcdf.cell2point_func import Cell2PointFunc
from examples.pihm2netcdf.mint_netcdf_write_func import MintNetCDFWriteFunc
from examples.pihm2netcdf.pihm_monthly_average_flooding_func import PihmMonthlyAverageFloodingFunc
from funcs import ReadFunc

if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent / "resources"

    pipeline = Pipeline(
        [ReadFunc, Cell2PointFunc, PihmMonthlyAverageFloodingFunc, MintNetCDFWriteFunc],
        wired=[
            ReadFunc.O.data == PihmMonthlyAverageFloodingFunc.I.graph,
            PihmMonthlyAverageFloodingFunc.O.data == MintNetCDFWriteFunc.I.data,
        ],
    )

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "points.model.yml",
        ReadFunc.I._1.resources: json.dumps({"surf": str(wdir / "surf.csv"), "points": str(wdir / "surf_points.csv")}),
        Cell2PointFunc.I.cell2point_file: wdir / "cell2points.R",
        Cell2PointFunc.I.cell_file: wdir / "GISlayer" / "Cells.shp",
        Cell2PointFunc.I.point_file: wdir / "surf_points.csv",
        PihmMonthlyAverageFloodingFunc.I.mean_space: 0.05,
        PihmMonthlyAverageFloodingFunc.I.start_time: datetime.strptime("2017-01-01 00:00:00", '%Y-%m-%d %H:%M:%S'),
        PihmMonthlyAverageFloodingFunc.I.end_time: datetime.strptime("2017-12-31 23:59:59", '%Y-%m-%d %H:%M:%S'),
        MintNetCDFWriteFunc.I.output_file: wdir / "MONTHLY_GRIDDED_SURFACE_INUNDATION_2017.nc",
        MintNetCDFWriteFunc.I.title: "Monthly gridded surface inundation for Pongo River in 2017",
        MintNetCDFWriteFunc.I.comment: "Outputs generated from the workflow",
        MintNetCDFWriteFunc.I.naming_authority: "edu.isi.workflow",
        MintNetCDFWriteFunc.I.id: "/MINT/NETCDF/MONTHLY_GRIDDED_SURFACE_INUNDATION_2017",
        MintNetCDFWriteFunc.I.creator_name: "Minh Pham",
        MintNetCDFWriteFunc.I.creator_email: "minhpham@usc.edu"
    }

    outputs = pipeline.exec(inputs)
