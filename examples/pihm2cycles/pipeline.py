from datetime import datetime
import os
from pathlib import Path

from dtran import Pipeline
from examples.pihm2cycles.cycle_write_func import CyclesWriteFunc
from examples.pihm2cycles.pihm2cycles_func import Pihm2CyclesFunc
from funcs import ReadFunc
from funcs.merge_func import MergeFunc
import ujson as json

if __name__ == "__main__":

    wdir = Path(os.path.abspath(__file__)).parent / "resources"

    pipeline = Pipeline(
        [ReadFunc, ReadFunc, ReadFunc, ReadFunc, MergeFunc, Pihm2CyclesFunc, CyclesWriteFunc],
        wired=[
            ReadFunc.O._1.data == MergeFunc.I.graph1,
            ReadFunc.O._2.data == MergeFunc.I.graph2,
            MergeFunc.O.data == Pihm2CyclesFunc.I.pihm_data_graph,
            ReadFunc.O._3.data == Pihm2CyclesFunc.I.pihm_soil_graph,
            ReadFunc.O._4.data == Pihm2CyclesFunc.I.pid_graph,
            Pihm2CyclesFunc.O.reinit_graph == CyclesWriteFunc.I.reinit_graph,
            Pihm2CyclesFunc.O.cycle_soil_graph == CyclesWriteFunc.I.soil_graph,
        ],
    )

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "pg_gw.model.yml",
        ReadFunc.I._1.resources: json.dumps(
            {"default": str(wdir / "pg.GW.csv"), "cycle_mapping": str(wdir / "cycles_soil.csv")}
        ),
        ReadFunc.I._2.repr_file: wdir / "pg_infil.model.yml",
        ReadFunc.I._2.resources: json.dumps(
            {"default": str(wdir / "pg.infil.csv"), "cycle_mapping": str(wdir / "cycles_soil.csv")}
        ),
        ReadFunc.I._3.repr_file: wdir / "pihm_soil.model.yml",
        ReadFunc.I._3.resources: wdir / "pihm_soil.csv",
        ReadFunc.I._4.repr_file: wdir / "cycles_soil.model.yml",
        ReadFunc.I._4.resources: wdir / "cycles_soil.csv",
        Pihm2CyclesFunc.I.cycles_layers: "[0.05, 0.05, 0.10, 0.2, 0.4, 0.4, 0.4, 0.4]",
        Pihm2CyclesFunc.I.patch_id: "1",
        Pihm2CyclesFunc.I.gw_depth: 30,
        CyclesWriteFunc.I.reinit_file: wdir / "cycles.REINIT",
        CyclesWriteFunc.I.soil_file: wdir / "cycles.soil",
    }

    outputs = pipeline.exec(inputs)
