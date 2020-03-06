#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import uuid
from dtran.argtype import ArgType
from dtran.backend import ShardedBackend
from dtran.ifunc import IFunc, IFuncType
from drepr import outputs
from funcs.gdal.raster import Raster, GeoTransform, BoundingBox, ReSample
from funcs.gdal.raster_to_dataset import raster_to_dataset
import fiona
from fiona.crs import from_epsg


class CroppingTransFunc(IFunc):
    id = "cropping trans"

    inputs = {
        "variable_name": ArgType.String,
        "dataset": ArgType.DataSet(None),
        "shape": ArgType.DataSet(None, optional=True),
        "xmin": ArgType.Number(optional=True),
        "ymin": ArgType.Number(optional=True),
        "xmax": ArgType.Number(optional=True),
        "ymax": ArgType.Number(optional=True),
    }

    outputs = {"data": ArgType.DataSet(None)}

    func_type = IFuncType.CROPPING_TRANS
    friendly_name: str = "Cropping function"
    example = {
        "variable_name": "",
        "xmin": "",
        "ymin": "",
        "xmax": "",
        "ymax": "",
    }

<<<<<<< HEAD
    def __init__(
            self, variable_name: str, dataset, shape=None, xmin=0, ymin=0, xmax=0, ymax=0
    ):
=======
    def __init__(self, variable_name: str, dataset, shape=None, xmin=0, ymin=0, xmax=0, ymax=0):
>>>>>>> upstream/master
        self.variable_name = variable_name
        self.dataset = dataset
        self.shape_sm = shape
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

        self.use_temp = True
        if self.shape_sm is None:
            self.use_bbox = True
        else:
            self.use_bbox = False
            if isinstance(self.shape_sm, str):
                self.use_temp = False

    @staticmethod
    def shape_array_to_shapefile(data, fname):
        polygon = data['polygon']
        if isinstance(polygon[0][0][0], (int, float)):
            shape_type = "Polygon"
        else:
            shape_type = "MultiPolygon"

        epsg = from_epsg(data['epsg'])
        driver = "ESRI Shapefile"
        polygon_data = {
            "geometry": {
                "type": shape_type,
                "coordinates": polygon
            },
            "properties": {
                "name": "TempCroppingPolygon"
            },
        }
        schema = {"geometry": shape_type, "properties": {"name": "str"}}
        with fiona.open(
                fname,
                "w",
                crs="+datum=WGS84 +ellps=WGS84 +no_defs +proj=longlat",
                driver=driver,
                schema=schema,
        ) as shapefile:
            shapefile.write(polygon_data)

    @staticmethod
    def get_namespaces(sm: ArgType.DataSet):
        mint_ns = sm.ns("https://mint.isi.edu/")
        mint_geo_ns = sm.ns("https://mint.isi.edu/geo")
        rdf_ns = sm.ns(outputs.Namespace.RDF)
        return mint_ns, mint_geo_ns, rdf_ns

    @staticmethod
    def extract_raster(sm: ArgType.DataSet, variable_name: str):
        mint_ns, mint_geo_ns, rdf_ns = CroppingTransFunc.get_namespaces(sm)

        rasters = []

        for c in sm.c(mint_ns.Variable).filter(
                outputs.FCondition(mint_ns.standardName, "==", variable_name)):
            for raster_id, sc in c.group_by(mint_geo_ns.raster):
                if sc.p(mint_ns.timestamp) is not None:
                    timestamp = next(sc.iter_records()).s(mint_ns.timestamp)
                else:
                    timestamp = None

                data = sc.p(rdf_ns.value).as_ndarray(
                    [sc.p(mint_geo_ns.lat), sc.p(mint_geo_ns.long)])
                gt_info = sm.get_record_by_id(raster_id)
                gt = GeoTransform(
                    x_0=gt_info.s(mint_geo_ns.x_0),
                    y_0=gt_info.s(mint_geo_ns.y_0),
                    dx=gt_info.s(mint_geo_ns.dx),
                    dy=gt_info.s(mint_geo_ns.dy),
                )
                raster = Raster(
                    data.data,
                    gt,
                    int(gt_info.s(mint_geo_ns.epsg)),
                    data.nodata.value if data.nodata is not None else None,
                )
                rasters.append({"raster": raster, "timestamp": timestamp})

        return rasters

    @staticmethod
    def extract_shape(sm: ArgType.DataSet):
        mint_ns, mint_geo_ns, rdf_ns = CroppingTransFunc.get_namespaces(sm)

        shapes = []
        for c in sm.c(mint_ns.Place):
            for place in c.iter_records():
                polygon = sm.get_record_by_id(place.s(mint_geo_ns.bounding)).s(rdf_ns.value)
                # epsg = int(record.s(mint_geo_ns.epsg))
                epsg = 4326

                shapes.append({"polygon": polygon, "epsg": epsg, "place": place})

        return shapes

    def _crop_boundbox(self):

        self.rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        bb = BoundingBox(x_min=self.xmin, y_min=self.ymin, x_max=self.xmax, y_max=self.ymax)

        self.results = ShardedBackend(len(self.rasters))
        for r in self.rasters:
            cropped_raster = r["raster"].crop(bounds=bb, resampling_algo=ReSample.BILINEAR)
            self.results.add(raster_to_dataset(cropped_raster, self.results.inject_class_id, timestamp=r["timestamp"]))

    def _crop_shape_dataset(self):
        self.rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        self.shapes = CroppingTransFunc.extract_shape(self.shape_sm)

        self.results = ShardedBackend(len(self.rasters) * len(self.shapes))
        for r in self.rasters:
            for shape in self.shapes:
                tempfile_name = f"/tmp/{uuid.uuid1()}.shp"
                CroppingTransFunc.shape_array_to_shapefile(shape, tempfile_name)
                cropped_raster = r["raster"].crop(
                    vector_file=tempfile_name, resampling_algo=ReSample.BILINEAR
                )
                os.remove(tempfile_name)
                place = shape['place']
                self.results.add(raster_to_dataset(cropped_raster, self.results.inject_class_id, place=place,
                                                   timestamp=r["timestamp"]))

    def crop_shape_shardedbackend(self):
        # TODO Stub for sharded backend later
        pass

    def validate(self):
        # TODO Implement after demo
        return True

    def exec(self):
        if self.use_bbox:
            self._crop_boundbox()
        else:
            self._crop_shape_dataset()

        return {"data": self.results}
