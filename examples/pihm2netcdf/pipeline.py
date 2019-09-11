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
        [ReadFunc, Cell2PointFunc, ReadFunc, PihmMonthlyFloodingFunc, MintNetCDFWriteFunc],
        wired=[
            ReadFunc.O._1.data == PihmMonthlyFloodingFunc.I.surf_graph,
            Cell2PointFunc.O.point_file == ReadFunc.I._2.resources,
            ReadFunc.O._1.data == PihmMonthlyFloodingFunc.I.point_graph,
            PihmMonthlyFloodingFunc.O.data == MintNetCDFWriteFunc.I.data,
        ],
    )

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "pihm_surf.model.yml",
        ReadFunc.I._1.resources: wdir / "surf.csv",
        Cell2PointFunc.I.cell2point_file: wdir / "cell2points.R",
        Cell2PointFunc.I.cell_file: wdir / "pg.infil.csv",
        Cell2PointFunc.I.point_file: wdir / "surf_points.csv",
        ReadFunc.I._2.repr_file: wdir / "points.model.yml",
    }

    outputs = pipeline.exec(inputs)
