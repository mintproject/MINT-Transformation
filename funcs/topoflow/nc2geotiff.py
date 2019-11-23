import glob
import os
from pathlib import Path
from typing import List, Dict, Tuple, Callable, Any, Optional
from datetime import datetime
import gdal, numpy as np, osr
from netCDF4 import Dataset
from tqdm.auto import tqdm
from dtran import IFunc, ArgType
from funcs.topoflow.topoflow.utils import regrid
from multiprocessing import Pool


class NC2GeoTiff(IFunc):
    id = "nc2geotiff"
    description = "Convert all netcdf file in one folder to geotiff file in another folder"

    inputs = {
        "input_dir": ArgType.String,
        "output_dir": ArgType.String,
        "var_name": ArgType.String,
        "no_data": ArgType.Number
    }
    outputs = {}

    def __init__(self, input_dir, output_dir, var_name, no_value: float):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.var_name = var_name
        self.no_value = float(no_value)

    def exec(self) -> dict:
        nc_files = sorted(glob.glob(os.path.join(self.input_dir, "*.nc*")))
        pool = Pool()
        args = [
            (nc_file, self.var_name, os.path.join(self.output_dir, f"{Path(nc_file).stem}.tif"))
            for nc_file in nc_files
        ]

        count = 0
        for _ in tqdm(pool.imap_unordered(nc2geotiff, args), total=len(args)):
            count += 1
        return {}

    def validate(self) -> bool:
        return True


def nc2geotiff(nc_file: str, var_name: str, out_file, out_nodata=0.0, verbose=False):
    logs = ["convert gpm file to geotiff: %s at %s" % (Path(nc_file).stem, datetime.now().strftime("%H:%M:%S"))]
    ### raster = gdal.Open("NETCDF:{0}:{1}".format(nc_file, var_name), gdal.GA_ReadOnly )
    raster = gdal.Open("NETCDF:{0}:{1}".format(nc_file, var_name))
    band = raster.GetRasterBand(1)
    ncols = raster.RasterXSize
    nrows = raster.RasterYSize
    proj = raster.GetProjectionRef()
    bounds = regrid.get_raster_bounds(raster)  ######
    nodata = band.GetNoDataValue()
    geotransform = raster.GetGeoTransform()
    logs.append("finish read metadata data at %s" % datetime.now().strftime("%H:%M:%S"))

    # ----------------
    # BINH: using netcdf to read data instead of gdal
    # array = band.ReadAsArray()
    ds = Dataset(nc_file, "r")
    # bottom-up on the y-axis (netcdf compare to gdal north-up 90 -> -90)
    variable = ds.variables[var_name][0][::-1]
    new_array = np.asarray(variable)
    # assert np.allclose(array, new_array, atol=1e-7)
    # print(">>>> MATCH!!!")
    array = new_array
    # ----------------
    logs.append("finish read array data at %s" % datetime.now().strftime("%H:%M:%S"))

    ## array = raster.ReadAsArray(0, 0, ds_in.RasterXSize, ds_in.RasterYSize)
    # ----------------------------------------------
    # Get geotransform for array in nc_file
    # Note:  These look strange, but are CORRECT.
    # ----------------------------------------------

    ulx = geotransform[0]
    xres = geotransform[1]
    xrtn = geotransform[2]
    # -----------------------
    uly = geotransform[3]
    yrtn = geotransform[4]  # (not yres !!)
    yres = geotransform[5]  # (not yrtn !!)
    raster = None  # Close the nc_file

    if verbose:
        print('array: min  =', array.min(), 'max =', array.max())
        print('array.shape =', array.shape)
        print('array.dtype =', array.dtype)
        print('array nodata =', nodata)
        w = np.where(array > nodata)
        nw = w[0].size
        print('array # data =', nw)
        print(' ')

    # ----------------------------------------------
    # Rotate the array; column major to row major
    # a           = [[7,4,1],[8,5,2],[9,6,3]]
    # np.rot90(a) = [[1,2,3],[4,5,6],[7,8,9]]
    # ----------------------------------------------
    ### array2 = np.transpose( array )
    array2 = np.rot90(array)  ### counter clockwise
    ncols2 = nrows
    nrows2 = ncols

    # -------------------------
    # Change the nodata value
    # -------------------------
    array2[array2 <= nodata] = out_nodata

    # -----------------------------------------
    # Build new geotransform & projectionRef
    # -----------------------------------------
    lrx = bounds[2]
    lry = bounds[1]
    ulx2 = lry
    uly2 = lrx
    xres2 = -yres
    yres2 = -xres
    xrtn2 = yrtn
    yrtn2 = xrtn
    geotransform2 = (ulx2, xres2, xrtn2, uly2, yrtn2, yres2)
    proj2 = proj

    if (verbose):
        print('geotransform  =', geotransform)
        print('geotransform2 =', geotransform2)

    logs.append("finish rotating and transforming netcdf data at %s" % datetime.now().strftime("%H:%M:%S"))
    # ------------------------------------
    # Write new array to a GeoTIFF file
    # ------------------------------------
    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(out_file, ncols2, nrows2, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform(geotransform2)
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(array2)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromWkt(proj2)
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()

    # ---------------------
    # Close the out_file
    # ---------------------
    outRaster = None
    logs.append("finish write geotiff data at %s" % datetime.now().strftime("%H:%M:%S"))
    print(">>>", "|**|".join(logs))
    return None