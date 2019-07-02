import os
from pathlib import Path
from dtran import Pipeline, IFunc, ArgType
from funcs import ReadFunc, FilterFunc, WriteFuncGraph, UnitTransFunc


if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent
    crop_names = {"Maize (white) - Retail", "Cassava - Retail", "Groundnuts (shelled) - Retail", "Sesame - Retail",
                  "Sorghum (white, imported) - Retail"}

    pipeline = Pipeline([
        ReadFunc,
        FilterFunc,
        UnitTransFunc,
        WriteFuncGraph
    ], wired=[
        ReadFunc.O.data == FilterFunc.I.data,
        FilterFunc.O.data == UnitTransFunc.I.graph,
        UnitTransFunc.O.graph == WriteFuncGraph.I.graph
    ])

    inputs = {
        ReadFunc.I.repr_file: wdir / "wfp_food_prices_south-sudan.model.yml",
        ReadFunc.I.resources: wdir / "wfp_food_prices_south-sudan.csv",
        FilterFunc.I.filter: "@type = 'qb:Observation' and "
                             "sdmx-attribute:refArea.contains('Aweil (Town)') and "
                             "sdmx-dimension:refPeriod = '2016-10-15' and "
                             f"dcat-dimension:thing in {str(crop_names)}",
        UnitTransFunc.I.unit_value: "dcat:measure_1_value",
        UnitTransFunc.I.unit_label: "sdmx-attribute:unitMeasure",
        UnitTransFunc.I.unit_desired: "$/kg",
        WriteFuncGraph.I.main_class: "qb:Observation",
        WriteFuncGraph.I.output_file: wdir / "output.csv"
    }

    outputs = pipeline.exec(inputs)
