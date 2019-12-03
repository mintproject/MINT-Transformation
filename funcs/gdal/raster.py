#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
import os

import numpy as np
from pathlib import Path
from osgeo import gdal, osr, gdal_array
from typing import Tuple, Union
from enum import Enum, IntEnum
from dataclasses import dataclass, astuple


@dataclass
class GeoTransform:
    # x = longitude, y = latitude
    # need to keep the order match with gdal: x_min,
    x_min: float = 0.0
    x_res: float = 1.0
    x_angle: float = 0.0
    y_min: float = 0.0
    y_angle: float = 0.0
    y_res: float = 1.0

    @staticmethod
    def from_gdal(t):
        return GeoTransform(x_min=t[0], x_res=t[1], x_angle=t[2], y_min=t[3], y_angle=t[4], y_res=t[5])

    def to_gdal(self):
        return self.x_min, self.x_res, self.x_angle, self.y_min, self.y_angle, self.y_res


class EPSG(IntEnum):
    WGS_84 = 4326
    WGS_84_PSEUDO_MERCATOR = 3857


@dataclass
class Bounds:
    # x = longitude, y = latitude
    x_min: float
    y_min: float
    x_max: float
    y_max: float

    def to_gdal(self) -> Tuple[float, float, float, float]:
        # min-long, min-lat, max-long, max-lat
        return self.x_min, self.y_min, self.x_max, self.y_max


class ReSample(Enum):
    NEAREST_NEIGHBOUR = 'nearest'
    BILINEAR = 'bilinear'


class Raster:
    def __init__(self, array: np.ndarray, geotransform: GeoTransform, epsg: EPSG, nodata: float=None):
        """
        @param nodata: which value should be no data
        """
        self.data = array
        self.geotransform = geotransform
        self.epsg = epsg
        self.nodata = nodata

        self.raster = gdal_array.OpenNumPyArray(array, True)
        self.raster.SetGeoTransform(astuple(geotransform))
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(epsg)
        self.raster.SetSpatialRef(srs)

    def __del__(self):
        self.raster = None
        self.data = None

    @staticmethod
    def from_geotiff(infile: str) -> 'Raster':
        ds = gdal.Open(infile)
        proj = osr.SpatialReference(wkt=ds.GetProjection())
        epsg = int(proj.GetAttrValue('AUTHORITY', 1))
        data = ds.ReadAsArray()
        nodata = set(ds.GetRasterBand(i).GetNoDataValue() for i in range(1, data.shape[0] + 1))
        assert len(nodata) == 1, "Do not support multiple no data value by now"
        nodata = list(nodata)[0]
        return Raster(data, GeoTransform.from_gdal(ds.GetGeoTransform()), epsg, nodata)

    def crop(self, bounds: Bounds = None, vector_file: Union[Path, str] = None, use_vector_bounds: bool = True,
             x_res: float = None, y_res: float = None, resampling_algo: ReSample = None) -> 'Raster':
        """
        @param x_res, y_res None will use original resolution
        """
        tmp_file = f"/vsimem/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S.%f')}.tif"
        warp_options = {'format': 'GTiff'}
        if vector_file is not None:
            warp_options['cutlineDSName'] = vector_file
            warp_options['cropToCutline'] = use_vector_bounds
            assert os.path.exists(vector_file)
        elif bounds is not None:
            warp_options['outputBounds'] = bounds.to_gdal()
        else:
            raise Exception('Please specify either bounds or vector_file to crop.')
        warp_options['xRes'] = x_res
        warp_options['yRes'] = y_res
        warp_options['srcNodata'] = self.nodata
        warp_options['resampleAlg'] = resampling_algo.value if resampling_algo is not None else None
        tmp_ds = gdal.Warp(tmp_file, self.raster, **warp_options)
        cropped_array, cropped_geotransform = tmp_ds.ReadAsArray(), GeoTransform.from_gdal(tmp_ds.GetGeoTransform())
        gdal.Unlink(tmp_file)

        return Raster(cropped_array, cropped_geotransform, self.epsg, self.nodata)

    def to_geotiff(self, outfile: str):
        driver = gdal.GetDriverByName("GTiff")
        bands, rows, cols = self.data.shape
        outdata = driver.Create(outfile, cols, rows, bands, gdal.GDT_UInt16)
        outdata.SetGeoTransform(self.raster.GetGeoTransform())
        outdata.SetProjection(self.raster.GetProjection())
        for band in range(bands):
            outdata.GetRasterBand(band + 1).WriteArray(self.data[band])
            if self.nodata is not None:
                outdata.GetRasterBand(band + 1).SetNoDataValue(self.nodata)
        outdata.FlushCache()


if __name__ == '__main__':
    raster = Raster.from_geotiff("/data/Sample/world.tif")
    # ethiopia = Bounds(32.75418, 3.22206, 47.98942, 15.15943)
    # raster = raster.crop(bounds=ethiopia, resampling_algo=ReSample.BILINEAR)
    # ethiopia = "/data/country_boundary/countries/ethiopia.shp"
    ethiopia = "/data/woredas/Warder.shp"
    raster = raster.crop(vector_file=ethiopia, resampling_algo=ReSample.BILINEAR)
    raster.to_geotiff("/data/Sample/somali.tif")

    # src_ds = gdal.Open("/data/Sample/world.tif")
    # proj = osr.SpatialReference(wkt=src_ds.GetProjection())
    # print(proj.GetAttrValue('AUTHORITY', 1))
    # print(src_ds.GetRasterBandCount())
    # print(src_ds.GetRasterBand(1).GetNoDataValue())
    # geoTransform = GeoTransform.from_gdal(src_ds.GetGeoTransform())
    # data = src_ds.ReadAsArray()
    # print(geoTransform, data.shape)
    # raster = Raster(data, geoTransform, EPSG.WGS_84, nodata=0)
    # # vector_file = "/data/Sample/ne_10m_admin_0_countries.shp"
    # # vector_file = "/data/debug/woredas/area_1.shp"
    # # vector_file = "/data/country_boundary/countries/ethiopia.shp"
    # vector_file = "/data/woredas/bio-jiganifado.shp"
    # x_res = 30 / 3600
    # y_res = 30 / 3600
    # x_res = None
    # y_res = None
    # grid, geoTransform = raster.crop(vector_file=vector_file, x_res=x_res, y_res=y_res)
    # grid = grid.astype(np.uint16)
    # print(grid.shape)
    # print(geoTransform)
    # print(src_ds.GetProjection())
    # print(raster.raster.GetProjection())
    # print(raster.GetNoDataValue())
    # driver = gdal.GetDriverByName("GTiff")
    # bands, rows, cols = grid.shape
    # print(grid[0].shape, grid.dtype)
    # print(np.max(grid))
    # outdata = driver.Create("/data/Sample/tmp.tif", cols, rows, bands, gdal.GDT_UInt16)
    # outdata.SetGeoTransform(geoTransform.to_gdal())
    # outdata.SetProjection(src_ds.GetProjection())
    # for band in range(bands):
    #     outdata.GetRasterBand(band+1).WriteArray(grid[band])
    #     # outdata.GetRasterBand(band).SetNoDataValue(10000)
    # outdata.FlushCache()
