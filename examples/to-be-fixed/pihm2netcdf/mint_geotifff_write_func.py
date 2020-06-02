import os
from pathlib import Path
from typing import Union

import numpy as np
import rasterio
import xarray as xr
from rasterio.transform import Affine

from dtran import IFunc, ArgType


class MintGeoTiffWriteFunc(IFunc):
    id = "geotiff_write_func"
    inputs = {
        "data": ArgType.NDimArray,
        "output_file": ArgType.String,
        "data_attr": ArgType.String(optional=True),
        "is_multiple_files": ArgType.Boolean(optional=True)
    }

    outputs = {
        "result": ArgType.Boolean
    }

    def __init__(
            self,
            data: xr.Dataset,
            output_file: Union[str, Path],
            data_attr: str = None,
            is_multiple_files: bool = False
    ):
        self.ndarray = data
        self.output_file = Path(output_file)
        if data_attr is None:
            self.data_attr = data_attr
        self.data_attr = list(self.ndarray.data_vars.keys())[0]
        self.is_multiple_files = is_multiple_files

    def exec(self) -> dict:
        data_array = self.ndarray[self.data_attr].values
        x_min = min(self.ndarray.coords["X"])
        x_max = max(self.ndarray.coords["X"])
        y_min = min(self.ndarray.coords["Y"])
        y_max = max(self.ndarray.coords["Y"])
        x_res = (x_max - x_min) / len(self.ndarray.coords["X"])
        y_res = (y_max - y_min) / len(self.ndarray.coords["Y"])
        transform = Affine.translation(x_min - x_res / 2, y_min - y_res / 2) * Affine.scale(x_res,
                                                                                            y_res)
        if self.is_multiple_files:
            os.makedirs(self.output_file, exist_ok=True)
            for i, time in enumerate(self.ndarray.coords["time"].values):
                with rasterio.open(
                        str(self.output_file / f"{time.replace(':', '-')}.tif"),
                        'w',
                        driver='GTiff',
                        height=data_array.shape[2],
                        width=data_array.shape[1],
                        count=1,
                        dtype=data_array.dtype,
                        crs='+init=epsg:4326',
                        transform=transform,
                ) as dst:
                    array = data_array[i].transpose()
                    dst.write(np.expand_dims(array, 0))
        else:
            with rasterio.open(
                    str(self.output_file),
                    'w',
                    driver='GTiff',
                    height=data_array.shape[2],
                    width=data_array.shape[1],
                    count=12,
                    dtype=data_array.dtype,
                    crs='+init=epsg:4326',
                    transform=transform,
            ) as dst:
                dst.write(data_array.swapaxes(1, 2))

        return {"result": True}

    def validate(self) -> bool:
        return True
