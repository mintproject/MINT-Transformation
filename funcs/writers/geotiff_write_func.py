import os
from pathlib import Path
from typing import Dict, Optional, Union
from zipfile import ZipFile

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
        "output_path": ArgType.String,
    }
    outputs = {
        "output_files": ArgType.ListString
    }

    def __init__(self, dataset: BaseOutputSM, variable_name: str, output_path: Union[str, Path]):
        self.dataset = dataset
        self.variable_name = variable_name
        self.output_path = os.path.abspath(str(output_path))

        if self.output_path.endswith(".zip"):
            self.dir_path = self.output_path[:-4]
        else:
            self.dir_path = self.output_path

        if not os.path.exists(self.output_path):
            Path(self.output_path).mkdir(exist_ok=True, parents=True)

    def exec(self):
        rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        rasters = sorted(rasters, key=lambda x: x['timestamp'])

        outfiles = [
            os.path.join(self.dir_path,
                         datetime.fromtimestamp(raster['timestamp'], tz=timezone.utc).strftime(f"%Y%m%d%H%M%S.{i}.tif"))
            for i, raster in enumerate(rasters)
        ]
        for outfile, raster in zip(outfiles, rasters):
            raster['raster'].to_geotiff(outfile)

        if self.output_path.endswith(".zip"):
            # compress the outfile
            with ZipFile(self.output_path, 'w') as z:
                for outfile in outfiles:
                    z.write(outfile, os.path.basename(outfile))
            return {"output_files": [self.output_path]}
        else:
            return {"output_files": outfiles}

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata
