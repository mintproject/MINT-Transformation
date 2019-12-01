#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
import numpy as np
from pathlib import Path
from osgeo import gdal, osr, gdal_array
from typing import Tuple, Union
from enum import Enum, IntEnum
from dataclasses import dataclass, astuple


@dataclass
class GeoTransform:
    x_min: float = 0.0
    x_res: float = 1.0
    x_angle: float = 0.0
    y_min: float = 0.0
    y_angle: float = 0.0
    y_res: float = 1.0


class EPSG(IntEnum):
    WGS_84 = 4326
    WGS_84_PSEUDO_MERCATOR = 3857


@dataclass
class Bounds:
    x_min: float
    y_min: float
    x_max: float
    y_max: float


class ReSample(str, Enum):
    NEAREST_NEIGHBOUR = 'nearest'
    BILINEAR = 'bilinear'


class Raster:
    def __init__(self, array: np.ndarray, geotransform: GeoTransform, epsg: EPSG):
        self.raster = gdal_array.OpenNumPyArray(array, True)
        self.raster.SetGeoTransform(astuple(geotransform))
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(epsg)
        self.raster.SetSpatialRef(srs)

    def __del__(self):
        self.raster = None

    def crop(self, bounds: Bounds = None, vector_file: Union[Path, str] = None, use_vector_bounds: bool = True,
             x_res: float = None, y_res: float = None, nodata: float = None, resampling_algo: ReSample = None) -> Tuple[np.ndarray, GeoTransform]:
        tmp_file = f"/vsimem/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S.%f')}.tif"
        warp_options = {'format': 'GTiff'}
        if vector_file is not None:
            warp_options['cutlineDSName'] = vector_file
            warp_options['cropToCutline'] = use_vector_bounds
        elif bounds is not None:
            warp_options['outputBounds'] = astuple(bounds)
        else:
            raise Exception('Please specify either bounds or vector_file to crop.')
        warp_options['xRes'] = x_res
        warp_options['yRes'] = y_res
        warp_options['srcNodata'] = nodata
        warp_options['resampleAlg'] = resampling_algo
        tmp_ds = gdal.Warp(tmp_file, self.raster, **warp_options)
        cropped_array, cropped_geotransform = tmp_ds.ReadAsArray(), GeoTransform(*tmp_ds.GetGeoTransform())
        gdal.Unlink(tmp_file)
        return cropped_array, cropped_geotransform
