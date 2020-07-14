from typing import Optional, Dict

import rioxarray

from dtran import IFunc, ArgType
from dtran.ifunc import IFuncType
from dtran.metadata import Metadata


class DEMCropFunc(IFunc):
    id = "dem_crop_func"
    description = """ A reader-transformation-writer multi-adapter.
    Crop a raster file by bounding box.
    """
    inputs = {
        "input_file": ArgType.String,
        "output_file": ArgType.String,
        "xmin": ArgType.Number,
        "ymin": ArgType.Number,
        "xmax": ArgType.Number,
        "ymax": ArgType.Number,
    }
    outputs = {"output_file": ArgType.String}
    friendly_name: str = "DEMCrop"
    func_type = IFuncType.MODEL_TRANS

    def __init__(self, input_file, output_file, xmin, ymin, xmax, ymax):
        self.input_file = input_file
        self.output_file = output_file
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        ds = rioxarray.open_rasterio(self.input_file)
        mask_lon = (ds.x >= self.xmin) & (ds.x <= self.xmax)
        mask_lat = (ds.y >= self.ymin) & (ds.y <= self.ymax)

        cropped_ds = ds.where(mask_lon & mask_lat, drop=True)
        cropped_ds.rio.to_raster(self.output_file)

        return {"output_file": self.output_file}

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata
