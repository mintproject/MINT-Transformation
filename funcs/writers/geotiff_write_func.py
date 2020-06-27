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
        "skip_on_exist": ArgType.Boolean,
    }
    outputs = {
        "output_files": ArgType.ListString
    }

    def __init__(self, dataset: BaseOutputSM, variable_name: str, output_dir: Union[str, Path], skip_on_exist: bool=False):
        self.dataset = dataset
        self.variable_name = variable_name
        self.output_dir = os.path.abspath(str(output_dir))
        self.skip_on_exist = skip_on_exist

        if not os.path.exists(self.output_dir):
            Path(self.output_dir).mkdir(exist_ok=True, parents=True)

    def exec(self):
        rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        rasters = sorted(rasters, key=lambda x: x['timestamp'])
        outfiles = [
            os.path.join(self.output_dir, datetime.fromtimestamp(raster['timestamp'], tz=timezone.utc).strftime(f"%Y%m%d%H%M%S.{i}.tif"))
            for i, raster in enumerate(rasters)
        ]

        for outfile, raster in zip(outfiles, rasters):
            if self.skip_on_exist and os.path.exists(outfile):
                continue
            raster['raster'].to_geotiff(outfile)

        return {"output_files": outfiles}

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata