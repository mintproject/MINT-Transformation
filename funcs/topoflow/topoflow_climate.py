#!/usr/bin/python
# -*- coding: utf-8 -*-
import glob
import re
from os.path import join
from pathlib import Path
from typing import Union, List, Optional, Dict

import gdal  # # ogr
import numpy as np
import os
import osr

from dtran.metadata import Metadata
from funcs.gdal.raster import BoundingBox
from funcs.topoflow.nc2geotiff import nc2geotiff
from tqdm import tqdm
from zipfile import ZipFile

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from funcs.topoflow.rti_files import generate_rti_file
from funcs.topoflow.topoflow_funcs import create_rts_rti


class Topoflow4ClimateWriteFunc(IFunc):
    id = "topoflow4_climate_write_func"
    description = '''A model-specific transformation. Prepare the topoflow RTS & RTI files.
    '''
    inputs = {
        "geotiff_files": ArgType.String,
        "cropped_geotiff_dir": ArgType.String,
        "output_file": ArgType.String,
        "bounds": ArgType.String,
        "xres_arcsecs": ArgType.Number,
        "yres_arcsecs": ArgType.Number,
        "unit_multiplier": ArgType.Number(optional=True)
    }
    outputs = {"output_file": ArgType.String}
    friendly_name: str = "Topoflow Climate"
    func_type = IFuncType.MODEL_TRANS
    example = {
        "geotiff_files": "/ws/examples/topoflow4/dev/data/geotiff/*.tif",
        "cropped_geotiff_dir": "/ws/examples/topoflow4/dev/data/geotiff_crop",
        "output_file": "/ws/examples/topoflow4/dev/data/output.zip",
        "bounds": "34.221249999999, 7.362083333332, 36.446249999999, 9.503749999999",
        "xres_arcsecs": "30",
        "yres_arcsecs": "30",
        "unit_multiplier": 1
    }

    def __init__(self, geotiff_files: str, cropped_geotiff_dir: str, output_file: str, bounds: str, xres_arcsecs: int, yres_arcsecs: int, unit_multiplier: float=1):
        x_min, y_min, x_max, y_max = [float(x.strip()) for x in bounds.split(",")]
        assert x_max > x_min and y_min < y_max
        self.bounding_box = BoundingBox(x_min, y_min, x_max, y_max)

        self.xres_arcsecs = xres_arcsecs
        self.yres_arcsecs = yres_arcsecs
        self.output_file = os.path.abspath(output_file)
        self.geotiff_files = geotiff_files
        self.cropped_geotiff_dir = os.path.abspath(cropped_geotiff_dir)
        self.unit_multiplier = unit_multiplier
        Path(self.cropped_geotiff_dir).mkdir(exist_ok=True, parents=True)
        Path(output_file).parent.mkdir(exist_ok=True, parents=True)

    def exec(self) -> dict:
        create_rts_rti(glob.glob(self.geotiff_files), self.output_file, self.cropped_geotiff_dir, self.bounding_box, self.xres_arcsecs, self.yres_arcsecs, self.unit_multiplier)
        return {"output_file": self.output_file}

    def validate(self) -> bool:
        return True