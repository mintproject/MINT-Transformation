import configparser
import os
import uuid
from pathlib import Path
from typing import Union

from drepr import Graph

from dtran import Pipeline, IFunc, ArgType
from funcs import ReadFunc, CSVWriteFunc


class TransWrapperFunc(IFunc):

    id = "cycle2crop"
    inputs = {
        "graph1": ArgType.Graph(None),
        "graph2": ArgType.Graph(None),
        "code": ArgType.FilePath,
        "filter": ArgType.String(optional=True),
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
            "Sorghum (white, imported) - Retail": "sorghum",
        }

        for crop_name, crop_new_name in crop_names.items():
            config = configparser.ConfigParser()
            config["DEFAULT"] = {"crop_name": crop_name, "percent-increase-fertilizer": 10}

            with open(exc_dir / f"config.{crop_new_name}.ini", "w") as f:
                config.write(f)

        return {}


if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent
    crop_names = {
        "Maize (white) - Retail",
        "Cassava - Retail",
        "Groundnuts (shelled) - Retail",
        "Sesame - Retail",
        "Sorghum (white, imported) - Retail",
    }

    pipeline = Pipeline(
        [ReadFunc, ReadFunc, TransWrapperFunc, CSVWriteFunc],
        wired=[
            ReadFunc.O._1.data == TransWrapperFunc.I.graph1,
            ReadFunc.O._2.data == TransWrapperFunc.I.graph2,
            TransWrapperFunc.O.data == CSVWriteFunc.I.graph,
        ],
    )

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "season.model.yml",
        ReadFunc.I._1.resources: wdir / "season.dat",
        ReadFunc.I._2.repr_file: wdir / "season.model.yml",
        ReadFunc.I._2.resources: wdir / "season1.dat",
        TransWrapperFunc.I._1.filter: "@type = 'qb:Observation' and sdmx-dimension:refPeriod = '2016-10-12'",
        TransWrapperFunc.I._2.filter: "@type = 'qb:Observation' and sdmx-dimension:refPeriod = '2016-10-12'",
        TransWrapperFunc.I.code: wdir / "cycles-to-crop.py",
        CSVWriteFunc.I.main_class: "qb:Observation",
        CSVWriteFunc.I.output_file: wdir / "output.csv",
        CSVWriteFunc.I.mapped_columns: {},
    }

    outputs = pipeline.exec(inputs)
