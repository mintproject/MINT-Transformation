from pathlib import Path
from typing import Union

import rasterio
import xarray as xr
from osgeo import gdal, osr
from rasterio.transform import Affine

from dtran import IFunc, ArgType


class MintGeoTiffWriteFunc(IFunc):
    id = "geotiff_write_func"
    inputs = {
        "data": ArgType.NDimArray,
        "output_file": ArgType.String,
        "data_attr": ArgType.String(optional=True)
    }

    outputs = {
        "result": ArgType.Boolean
    }

    def __init__(
            self,
            data: xr.Dataset,
            output_file: Union[str, Path],
            data_attr: str = None
    ):
        self.ndarray = data
        self.output_file = Path(output_file)
        if data_attr is None:
            self.data_attr = data_attr
        self.data_attr = list(self.ndarray.data_vars.keys())[0]

    def exec(self) -> dict:
        data_array = self.ndarray[self.data_attr].values
        x_min = min(self.ndarray.coords["X"])
        x_max = max(self.ndarray.coords["X"])
        y_min = max(self.ndarray.coords["Y"])
        res = (x_max - x_min) / 240.0
        transform = Affine.translation(x_min - res / 2, y_min - res / 2) * Affine.scale(res, res)
        with rasterio.open(
                str(self.output_file),
                'w',
                driver='GTiff',
                height=data_array.shape[0],
                width=data_array.shape[1],
                count=12,
                dtype=data_array.dtype,
                crs='+init=epsg:4326',
                transform=transform,
        ) as dst:
            dst.write(data_array)

        return {"result": True}

    def gdal_tif(self):
        dst_ds = gdal.GetDriverByName('GTiff').Create(str(self.output_file),
                                                      len(self.ndarray.coords["Y"]), len(self.ndarray.coords["X"]),
                                                      len(self.ndarray.coords["time"]),
                                                      gdal.GDT_Float32)
        x_min = min(self.ndarray.coords["X"])
        x_max = max(self.ndarray.coords["X"])
        y_min = max(self.ndarray.coords["Y"])
        y_max = max(self.ndarray.coords["Y"])
        x_res = (x_max - x_min) / len(self.ndarray.coords["X"])
        y_res = (y_max - y_min) / len(self.ndarray.coords["Y"])

        geo_transform = (x_min, x_res, 0, y_max, 0, -y_res)

        dst_ds.SetGeoTransform(geo_transform)  # specify coords
        srs = osr.SpatialReference()  # establish encoding
        srs.ImportFromEPSG(4326)  # WGS84 lat/long
        dst_ds.SetProjection(srs.ExportToWkt())  # export coords to file

        for time_index in range(self.ndarray.dims["time"]):
            var_data = self.ndarray[self.data_attr].isel(time=[time_index])

            dst_ds.GetRasterBand(time_index + 1).WriteArray(var_data.data[0])
            dst_ds.GetRasterBand(time_index + 1).SetNoDataValue(-999)

        dst_ds.FlushCache()  # write to disk

    def validate(self) -> bool:
        return True
