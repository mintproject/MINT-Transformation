from dtran import Pipeline
from funcs import ReadFunc

inputs = {
    "read_func__repr_file": "./examples/s01_ethiopia_commodity_price.yml",
    "read_func__resources": "./examples/s01_ethiopia_commodity_price.csv"
}

pipeline = Pipeline([
    ReadFunc
])
pipeline.exec(inputs)