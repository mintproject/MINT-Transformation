#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import List, Union

import numpy as np
from drepr import NDArrayGraph

from funcs.gdal.raster import Raster, GeoTransform, EPSG, BoundingBox, ReSample
from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class GdalCropFunc(IFunc):
    id = "gdal_crop_func"
    description = '''
        A transformation adapter.
        Crops an NDimArray using GDAL based on a bounding box or a vector file.
    '''
    inputs = {
        "data": ArgType.NDArrayGraph,
        "variable": ArgType.String,
        "bounds": ArgType.OrderedDict(optional=True),
        "vector_file": ArgType.FilePath(optional=True),
        "x_res": ArgType.Number(optional=True),
        "y_res": ArgType.Number(optional=True)
    }
    outputs = {
        "data": ArgType.NDArrayGraph
    }

    def __init__(self, data: Union[NDArrayGraph, List[NDArrayGraph]], variable: str, bounds: dict = None, vector_file: str = None, x_res: float = None, y_res: float = None):
        self.data = data
        self.x_res = x_res
        self.y_res = y_res
        self.variable = variable

        self.vector_file = None
        if vector_file is not None:
            self.vector_file = str(vector_file)

        self.bounds = None
        if bounds is not None:
            self.bounds = BoundingBox(**bounds)

        # now construct the raster or point model depends on the graph

    def exec(self):
        # TODO: logic to get geotransform, epsg metadata from input array
        raster = Raster(self.data, GeoTransform(0, 1, 0, 0, 0, 1), EPSG.WGS_84)
        array, geotransform = raster.crop(self.bounds, self.vector_file, True, self.x_res, self.y_res, None, ReSample.BILINEAR)
        # TODO: logic to add geotransform, epsg metadata to output array
        return {
            "data": array
        }

    def validate(self) -> bool:
        return True
