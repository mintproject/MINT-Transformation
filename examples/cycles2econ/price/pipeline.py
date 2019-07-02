import os
from pathlib import Path
from typing import Union

import ujson

from dtran import Pipeline, IFunc, ArgType
from funcs import ReadFunc, FilterFunc, WriteFuncGraph, UnitTransFunc, GraphStr2StrFunc


class PriceWriter(IFunc):
    id = "econ_price_writer"
    inputs = {"graph": ArgType.Graph(None), "output_file": ArgType.FilePath}
    outputs = {}

    def __init__(self, graph, output_file: Union[str, Path]):
        self.graph = graph
        self.output_file = str(output_file)

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        print(self.graph.nodes)
        data = [["", "p"]]
        for node in self.graph.nodes:
            data.append([node.data['dcat-dimension:thing'], node.data['dcat:measure_1_value']])

        with open(self.output_file, "w") as f:
            for r in data:
                f.write(",".join(r) + "\n")


if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent
    crop_names = {
        "Maize (white) - Retail": "maize",
        "Cassava - Retail": "cassava",
        "Groundnuts (shelled) - Retail": "groundnuts", "Sesame - Retail": "sesame",
        "Sorghum (white, imported) - Retail": "sorghum"
    }

    pipeline = Pipeline([
        ReadFunc,
        FilterFunc,
        GraphStr2StrFunc,
        UnitTransFunc,
        PriceWriter
    ], wired=[
        ReadFunc.O.data == FilterFunc.I.data,
        FilterFunc.O.data == GraphStr2StrFunc.I.graph,
        FilterFunc.O.data == UnitTransFunc.I.graph,
        UnitTransFunc.O.graph == PriceWriter.I.graph
    ])

    inputs = {
        ReadFunc.I.repr_file: wdir / "wfp_food_prices_south-sudan.model.yml",
        ReadFunc.I.resources: wdir / "wfp_food_prices_south-sudan.csv",
        FilterFunc.I.filter: "@type = 'qb:Observation' and "
                             "sdmx-attribute:refArea.contains('Aweil (Town)') and "
                             "sdmx-dimension:refPeriod = '2016-10-15' and "
        f"dcat-dimension:thing in {str(set(crop_names.keys()))}",
        GraphStr2StrFunc.I.semantic_type: "qb:Observation--dcat-dimension:thing",
        GraphStr2StrFunc.I.str2str: ujson.dumps(crop_names),
        UnitTransFunc.I.unit_value: "dcat:measure_1_value",
        UnitTransFunc.I.unit_label: "sdmx-attribute:unitMeasure",
        UnitTransFunc.I.unit_desired: "$/kg",
        PriceWriter.I.output_file: wdir / "price.csv"
    }

    outputs = pipeline.exec(inputs)
