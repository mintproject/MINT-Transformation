from dtran import IFunc, ArgType
from osgeo import gdal


class InterpolateFunc(IFunc):
    inputs = {
        "data": ArgType.NDimArray,
        "lat_attr": ArgType.String,
        "long_attr": ArgType.String,
        "main_attr": ArgType.String,
        "method": ArgType.String
    }

    outputs = {
        "data": ArgType.NDimArray
    }

    def exec(self) -> dict:
        pass

    def interpolate(self):
        drv = gdal.GetDriverByName("VRT")
