import os
from pathlib import Path
from typing import Dict, Optional, Union

from drepr.outputs.base_output_sm import BaseOutputSM
from datetime import datetime, timezone
from dtran import ArgType
from dtran.ifunc import IFunc
from dtran.metadata import Metadata
from funcs.gdal.trans_cropping_func import CroppingTransFunc


class GeoTiffWriteFunc(IFunc):
    id = "geotiff_write_func"
    description = """Write dataset to GeoTiff format."""
    inputs = {
        "dataset": ArgType.DataSet(None),
        "variable_name": ArgType.String,
        "output_dir": ArgType.String,
    }
    outputs = {
        "output_files": ArgType.String
    }

    def __init__(self, dataset: BaseOutputSM, variable_name: str, output_dir: Union[str, Path]):
        self.dataset = dataset
        self.variable_name = variable_name
        self.output_dir = os.path.abspath(str(output_dir))

    def exec(self):
        rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        rasters = sorted(rasters, key=lambda x: x['timestamp'])
        names = [
            datetime.fromtimestamp(raster['timestamp'], tz=timezone.utc).strftime(f"%Y%m%d%H%M%S.{i}.tif")
            for i, raster in enumerate(rasters)
        ]
        for name, raster in zip(names, rasters):
            raster['raster'].to_geotiff(os.path.join(self.output_dir, name))

        return { "output_files": os.path.join(self.output_dir, "*.tif") }

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata