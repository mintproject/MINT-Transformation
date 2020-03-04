#!/usr/bin/python
# -*- coding: utf-8 -*-

from dtran.argtype import ArgType
from funcs.readers.dcat_read_func import ShardedBackend
from dtran.ifunc import IFunc
from drepr import outputs
from funcs.gdal.raster import Raster, GeoTransform, BoundingBox, ReSample
from funcs.gdal.raster_to_dataset import raster_to_dataset
import fiona
from fiona.crs import from_epsg

# TODO Inelegant solution to temp files, should probably clean them up after execution
tempfile_name = "temp_shape.shp"


class CroppingTransFunc(IFunc):
    id = "cropping trans"

    inputs = {
        "variable_name": ArgType.String,
        "dataset": ArgType.DataSet,
        "shape": ArgType.DataSet,
        "xmin": ArgType.Number,
        "ymin": ArgType.Number,
        "xmax": ArgType.Number,
        "ymax": ArgType.Number
    }

    outputs = {"array": ArgType.DataSet(None)}

    def __init__(self, variable_name: str, dataset, shape, xmin: int, ymin: int, xmax: int,
                 ymax: int):
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
        if len(data[0]) == 2:
            shape_type = 'Polygon'
        else:
            shape_type = 'MultiPolygon'

        epsg = from_epsg(data[1])
        driver = "ESRI Shapefile"
        polygon = {
            'geometry': {
                'type': shape_type,
                'coordinates': data[0]
            },
            'properties': {
                'name': 'TempCroppingPolygon'
            }
        }
        schema = {'geometry': shape_type, 'properties': {'name': 'str'}}
        with fiona.open(fname, 'w', crs=epsg, driver=driver, schema=schema) as shapefile:
            shapefile.write(polygon)

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
                data = sc.p(rdf_ns.value).as_ndarray(
                    [sc.p(mint_geo_ns.lat), sc.p(mint_geo_ns.long)])
                gt_info = sm.get_record_by_id(raster_id)
                gt = GeoTransform(x_0=gt_info.s(mint_geo_ns.x_0),
                                  y_0=gt_info.s(mint_geo_ns.y_0),
                                  dx=gt_info.s(mint_geo_ns.dx),
                                  dy=gt_info.s(mint_geo_ns.dy))
                raster = Raster(data.data, gt, int(gt_info.s(mint_geo_ns.epsg)),
                                data.nodata.value if data.nodata is not None else None)

                rasters.append(raster)

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

                shapes.append([polygon, epsg, place])

        return shapes

    def _crop_boundbox(self):
        self.rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        bb = BoundingBox(x_min=self.xmin, y_min=self.ymin, x_max=self.xmax, y_max=self.ymax)

        self.results = []
        for r in self.rasters:
            cropped_raster = r.crop(bounds=bb, resampling_algo=ReSample.BILINEAR)
            self.results.append(raster_to_dataset(cropped_raster))

    def _crop_shape_dataset(self):
        self.rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        self.shapes = CroppingTransFunc.extract_shape(self.shape_sm)

        self.results = []
        for r in self.rasters:
            for s in self.shapes:
                CroppingTransFunc.shape_array_to_shapefile(s, tempfile_name)
                cropped_raster = r.crop(vector_file=tempfile_name,
                                        resampling_algo=ReSample.BILINEAR)
                place = s[2]
                self.results.append(raster_to_dataset(cropped_raster, place))

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

        self.output = ShardedBackend(len(self.results))
        for d in self.results:
            self.output.add(d)
        return self.output
