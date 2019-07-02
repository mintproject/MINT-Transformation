import os
from pathlib import Path
from dtran import Pipeline, IFunc, ArgType
from funcs import ReadFunc, FilterFunc, WriteFuncGraph


crop_names = set([
    "Maize (white) - Retail",
    "Cassava - Retail",
    "Groundnuts (shelled) - Retail",
    "Sesame - Retail",
    "Sorghum (white, imported) - Retail"
])

def filter_func(n) -> bool:
    global crop_names

    return (n.data['@type'] == 'qb:Observation') and \
        (n.data['sdmx-attribute:refArea'].endswith('Aweil (Town)')) and \
        (n.data["dcat-dimension:thing"] in crop_names)


if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent

    pipeline = Pipeline([
        ReadFunc,
        FilterFunc,
        WriteFuncGraph
    ], wired=[
        ReadFunc.O.data == FilterFunc.I.data,
        FilterFunc.O.data == WriteFuncGraph.I.graph
    ])

    inputs = {
        ReadFunc.I.repr_file: wdir / "wfp_food_prices_south-sudan.model.yml",
        ReadFunc.I.resources: wdir / "wfp_food_prices_south-sudan.csv",
        FilterFunc.I.filter: filter_func,
        WriteFuncGraph.I.main_class: "qb:Observation",
        WriteFuncGraph.I.output_file: wdir / "output.csv"
    }

    outputs = pipeline.exec(inputs)
    print(outputs)