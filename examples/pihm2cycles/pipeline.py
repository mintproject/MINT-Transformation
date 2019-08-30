import os
from pathlib import Path
from typing import Union

import ujson

from dtran import Pipeline, IFunc, ArgType
from funcs import ReadFunc, FilterFunc, WriteFuncGraph, UnitTransFunc, GraphStr2StrFunc
from funcs.trans_lambda_func import LambdaTransFunc


def get_saturation_fraction(top: float, bottom: float, water_level: float) -> \
        float:
    if water_level > bottom:
        return 0.0
    if water_level < top:
        return 1.0
    return (bottom - water_level) * 1.0 / (bottom - top)

if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent

    pipeline = Pipeline([ReadFunc, FilterFunc, LambdaTransFunc, LambdaTransFunc, WriteFuncGraph],
                        wired=[
                            ReadFunc.O.data == FilterFunc.I.data,
                            FilterFunc.O.data == LambdaTransFunc.I._1.graph,
                            LambdaTransFunc.O._1.data == UnitTransFunc.I.graph,
                            UnitTransFunc.O.graph == WriteFuncGraph.I.graph
                        ])

    saturation_function =

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
        WriteFuncGraph.I.main_class: 'qb:Observation',
        WriteFuncGraph.I.mapped_columns: {
            "dcat-dimension:thing": "",
            "dcat:measure_1_value": "p"
        },
        WriteFuncGraph.I.output_file: wdir / "price.csv"
    }

    outputs = pipeline.exec(inputs)
