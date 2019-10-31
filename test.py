from typing import List, Dict, Tuple, Callable, Any, Optional
import gdal, numpy as np
from datetime import datetime
from netCDF4 import Dataset

var_name = "HQprecipitation"
nc_file = "/data/mint/3B-HHR-E.MS.MRG.3IMERG.20140101-S000000-E002959.0000.V06B.HDF5.nc4"
# nc_file = "/data/small_fake.nc4"

def gdal_readnc():
    print("run code at %s" % datetime.now().strftime("%H:%M:%S"))
    raster = gdal.Open("NETCDF:{0}:{1}".format(nc_file, var_name))
    band = raster.GetRasterBand(1)
    ncols = raster.RasterXSize
    nrows = raster.RasterYSize
    proj = raster.GetProjectionRef()
    nodata = band.GetNoDataValue()
    geotransform = raster.GetGeoTransform()
    print("finish read metadata data at %s" % datetime.now().strftime("%H:%M:%S"))

    array = band.ReadAsArray()
    print("finish read raster data data at %s" % datetime.now().strftime("%H:%M:%S"))
    array = array.reshape((nrows, ncols))
    return array, nodata


def netcdf_read():
    ds = Dataset(nc_file, "r")
    variable = ds.variables[var_name]
    new_array = np.asarray(variable[0] if len(variable.shape) > 2 else variable)
    return new_array


def report_match(arr1, arr2, nodata, tol=1e-7):
    equal_points = (np.abs(arr1 - arr2) < tol).astype(np.float32)
    arr1_msk = (arr1 != nodata).astype(dtype=np.float32)
    arr2_msk = (arr2 != nodata).astype(dtype=np.float32)

    print("#equal points =", np.sum(equal_points))
    print("#equal points (ignore nodata) =", np.sum(equal_points * arr1_msk * arr2_msk))
    print("%equal points =", np.mean(equal_points))
    print("%equal points (ignore nodata) =", np.sum(equal_points * arr1_msk * arr2_msk) / np.sum(arr1_msk * arr2_msk))


gdal_array, nodata = gdal_readnc()
netcdf_array = netcdf_read()
report_match(gdal_array, netcdf_array, nodata)
report_match(gdal_array[::-1], netcdf_array, nodata)

