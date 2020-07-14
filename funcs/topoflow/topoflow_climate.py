#!/usr/bin/python
# -*- coding: utf-8 -*-
import glob
import os
from pathlib import Path
from typing import Union, List
from zipfile import ZipFile

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from funcs.gdal.raster import BoundingBox
from funcs.topoflow.topoflow_funcs import create_rts_rti


from typing import *
from inspect import signature, isgeneratorfunction

from dtran.ifunc import IFunc
from dtran.metadata import Metadata


class Topoflow4ClimateWriteFunc(IFunc):
    id = "topoflow4_climate_write_func"
    description = '''A model-specific transformation. Prepare the topoflow RTS & RTI files.
    '''
    inputs = {
        "geotiff_files": ArgType.ListString,
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

    def __init__(self, geotiff_files: List[str], cropped_geotiff_dir: str, output_file: str, bounds: str, xres_arcsecs: int, yres_arcsecs: int, unit_multiplier: float=1):
        x_min, y_min, x_max, y_max = [float(x.strip()) for x in bounds.split(",")]
        assert x_max > x_min and y_min < y_max
        self.bounding_box = BoundingBox(x_min, y_min, x_max, y_max)

        self.xres_arcsecs = xres_arcsecs
        self.yres_arcsecs = yres_arcsecs
        self.output_file = os.path.abspath(output_file)
        if isinstance(geotiff_files, str):
            geotiff_files = glob.glob(self.geotiff_files)
        self.geotiff_files = geotiff_files
        self.cropped_geotiff_dir = os.path.abspath(cropped_geotiff_dir)

        if not os.path.exists(self.cropped_geotiff_dir):
            Path(self.cropped_geotiff_dir).mkdir(exist_ok=True, parents=True)

        self.unit_multiplier = unit_multiplier
        Path(self.cropped_geotiff_dir).mkdir(exist_ok=True, parents=True)
        Path(output_file).parent.mkdir(exist_ok=True, parents=True)

        assert not os.path.exists(output_file)

    def exec(self) -> dict:
        if self.output_file.endswith(".zip"):
            rts_file = self.output_file.replace(".zip", ".rts")
            rti_file = self.output_file.replace(".zip", ".rti")
        else:
            rts_file = self.output_file

        create_rts_rti(self.geotiff_files, rts_file, self.cropped_geotiff_dir, self.bounding_box, self.xres_arcsecs, self.yres_arcsecs, self.unit_multiplier)

        if self.output_file.endswith(".zip"):
            # compress the outfile
            with ZipFile(self.output_file, 'w') as z:
                z.write(rts_file, os.path.basename(rts_file))
                z.write(rti_file, os.path.basename(rti_file))
        return {"output_file": self.output_file}

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata


class Topoflow4ClimateWriteWrapperFunc(IFunc):
    func_cls = Topoflow4ClimateWriteFunc
    id = Topoflow4ClimateWriteFunc.id
    description = Topoflow4ClimateWriteFunc.description
    inputs = Topoflow4ClimateWriteFunc.inputs
    outputs = Topoflow4ClimateWriteFunc.outputs
    func_type = Topoflow4ClimateWriteFunc.func_type
    friendly_name = Topoflow4ClimateWriteFunc.friendly_name
    example = Topoflow4ClimateWriteFunc.example

    def __init__(self, **kwargs):
        try:
            signature(Topoflow4ClimateWriteWrapperFunc.func_cls.__init__).bind(Topoflow4ClimateWriteWrapperFunc.func_cls, **kwargs)
        except TypeError:
            print(f"Cannot initialize cls: {Topoflow4ClimateWriteWrapperFunc.func_cls}")
            raise
        self.func_args = kwargs

    async def exec(self) -> Union[dict, Generator[dict, None, None], AsyncGenerator[dict, None]]:
        func_args = self.func_args.copy()

        geotiff_files = []
        async for infiles in func_args['geotiff_files']:
            geotiff_files += infiles
        func = self.func_cls(**{**func_args, 'geotiff_files': geotiff_files})
        func.get_preference = self.get_preference
        try:
            yield func.exec()
        except AssertionError as e:
            print(e)

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata

