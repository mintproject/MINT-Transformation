import os
from pathlib import Path
from dtran import Pipeline, IFunc, ArgType
from funcs import ReadFunc, FilterFunc, WriteFuncGraph, UnitTransFunc


class TransWrapperFunc(IFunc):
    pass


if __name__ == "__main__":
    wdir = Path(os.path.abspath(__file__)).parent
    crop_names = {"Maize (white) - Retail", "Cassava - Retail", "Groundnuts (shelled) - Retail", "Sesame - Retail",
                  "Sorghum (white, imported) - Retail"}

    pipeline = Pipeline([
        ReadFunc,
        FilterFunc,
        ReadFunc,
        FilterFunc,
        # TransWrapperFunc
    ], wired=[
        ReadFunc.O._1.data == FilterFunc.I._1.data,
        ReadFunc.O._2.data == FilterFunc.I._2.data,
        # FilterFunc.O._1.data == TransWrapperFunc.I.graph1,
        # FilterFunc.O._2.data == TransWrapperFunc.I.graph2,
    ])

    inputs = {
        ReadFunc.I._1.repr_file: wdir / "season.model.yml",
        ReadFunc.I._1.resources: wdir / "season.dat",
        # ReadFunc.I.resources: wdir / "season.dat",
        # ReadFunc.I._2.repr_file: wdir / "season.model.yml",
        #
        # FilterFunc.I.filter: "@type = 'qb:Observation' and "
        #                      "sdmx-attribute:refArea.contains('Aweil (Town)') and "
        #                      "sdmx-dimension:refPeriod = '2016-10-15' and "
        #                      f"dcat-dimension:thing in {str(crop_names)}",
        # UnitTransFunc.I.unit_value: "dcat:measure_1_value",
        # UnitTransFunc.I.unit_label: "sdmx-attribute:unitMeasure",
        # UnitTransFunc.I.unit_desired: "$/kg",
        # WriteFuncGraph.I.main_class: "qb:Observation",
        # WriteFuncGraph.I.output_file: wdir / "output.csv"
    }

    outputs = pipeline.exec(inputs)
