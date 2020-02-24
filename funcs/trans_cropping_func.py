#!/usr/bin/python
# -*- coding: utf-8 -*-

from dtran.argtype import ArgType, Optional
from dtran.ifunc import IFunc
from numpy import ndarray
from drepr import DRepr, outputs
from extra_libs.raster import Raster, GeoTransform, EPSG, BoundingBox, ReSample

class CroppingTransFunc(IFunc):
    id = "cropping trans"

    inputs = {
        "variable_name": ArgType.String,
        "dataset": ArgType.DataSet,
        "xmin": ArgType.Number,
        "ymin": ArgType.Number,
        "xmax": ArgType.Number,
        "ymax": ArgType.Number
    }

    outputs = {"array": ArgType.NDimArray(None)}

    def __init__(
        self, variable_name: str, dataset: str, xmin: int, ymin: int, xmax: int, ymax: int
    ):
        self.variable_name = variable_name
        self.dataset = dataset
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

        self.sm = self.dataset
        # Namespaces
        self.mint_ns = self.sm.ns("https://mint.isi.edu/")
        self.mint_geo_ns = self.sm.ns("https://mint.isi.edu/geo")
        self.rdf_ns = self.sm.ns(outputs.Namespace.RDF)

    def exec(self):
        for c in self.sm.c(self.mint_ns.Variable).filter(outputs.FCondition(self.mint_ns.standardName, "==", self.variable_name)):
            for raster_id, sc in c.group_by(self.mint_geo_ns.raster):
                data = sc.p(self.rdf_ns.value).as_ndarray([sc.p(self.mint_geo_ns.lat), sc.p(self.mint_geo_ns.long)])
                gt_info = self.sm.get_record_by_id(raster_id)

                if data.index_props[0].size > 1 and data.index_props[0][1] > data.index_props[0][0]:
                    data.data = data.data[::-1]
                    data.index_props[0] = data.index_props[0][::-1]

                gt = GeoTransform(x_min=gt_info.s(self.mint_geo_ns.x_min),
                                  y_max=gt_info.s(self.mint_geo_ns.y_min) + gt_info.s(self.mint_geo_ns.y) * data.data.shape[0],
                                  dx=gt_info.s(self.mint_geo_ns.dx), dy=-gt_info.s(self.mint_geo_ns.y))

                bb = BoundingBox(x_min=self.xmin, y_min=self.ymin, x_max=self.xmax, y_max=self.ymax)

                raster = Raster(data.data, gt, int(gt_info.s(self.mint_geo_ns.epsg)))

                cropped_raster = raster.crop(bounds=BoundingBox)

                return cropped_raster.data
