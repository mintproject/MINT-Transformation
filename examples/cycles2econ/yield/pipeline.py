import configparser
import os
import uuid
from pathlib import Path
from typing import Union

from pydrepr import Graph

from dtran import Pipeline, IFunc, ArgType
from funcs import ReadFunc, FilterFunc, WriteFuncGraph, UnitTransFunc


class TransWrapperFunc(IFunc):

    id = "cycle2crop"
    inputs = {
        "graph1": ArgType.Graph(None),
        "graph2": ArgType.Graph(None),
        "code": ArgType.FilePath
    }
    outputs = {}

    def __init__(self, graph1: Graph, graph2: Graph, code: Union[str, Path]):
        self.graph1 = graph1
        self.graph2 = graph2
        self.code = code

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        exc_dir = Path("/tmp/" + str(uuid.uuid4()).replace("-", ""))
        exc_dir.mkdir()

        crop_names = {
            "Maize (white) - Retail": "maize",
            "Cassava - Retail": "cassava",
            "Groundnuts (shelled) - Retail": "groundnuts",
            "Sesame - Retail": "sesame",
            "Sorghum (white, imported) - Retail": "sorghum"
        }

        for crop_name, crop_new_name in crop_names.items():
            config = configparser.ConfigParser()
            config['DEFAULT'] = {
                "crop_name": crop_name,
                "percent-increase-fertilizer": 10
            }

            with open(exc_dir / f"config.{crop_new_name}.ini", "w") as f:
                config.write(f)

        return {}
        # WriteFuncGraph(self.graph1, "qb:Observation", f"/tmp/{self.id}.graph.csv", {
        #     ""
        # })


if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent
    crop_names = {
        "Maize (white) - Retail", "Cassava - Retail", "Groundnuts (shelled) - Retail",
        "Sesame - Retail", "Sorghum (white, imported) - Retail"
    }

    pipeline = Pipeline(
        [ReadFunc, FilterFunc, ReadFunc, FilterFunc, TransWrapperFunc, WriteFuncGraph],
        wired=[
            ReadFunc.O._1.data == FilterFunc.I._1.data, ReadFunc.O._2.data == FilterFunc.I._2.data,
            FilterFunc.O._1.data == TransWrapperFunc.I.graph1,
            FilterFunc.O._2.data == TransWrapperFunc.I.graph2,
            FilterFunc.O._1.data == WriteFuncGraph.I.graph
        ])

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "season.model.yml",
        ReadFunc.I._1.resources: wdir / "season.dat",
        ReadFunc.I._2.repr_file: wdir / "season.model.yml",
        ReadFunc.I._2.resources: wdir / "season1.dat",

        FilterFunc.I._1.filter: "@type = 'qb:Observation' and sdmx-dimension:refPeriod = '2016-10-12'",
        FilterFunc.I._2.filter: "@type = 'qb:Observation' and sdmx-dimension:refPeriod = '2016-10-12'",

        TransWrapperFunc.I.code: wdir / "cycles-to-crop.py",
        WriteFuncGraph.I.main_class: "qb:Observation",
        WriteFuncGraph.I.output_file: wdir / "output.csv",
        WriteFuncGraph.I.mapped_columns: {}
    }

    outputs = pipeline.exec(inputs)
