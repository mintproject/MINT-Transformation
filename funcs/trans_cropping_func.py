#!/usr/bin/python
# -*- coding: utf-8 -*-

from dtran.argtype import ArgType, Optional
from dtran.ifunc import IFunc
from numpy import array
from drepr import DRepr, outputs
from extra_libs.raster import Raster, GeoTransform, EPSG, BoundingBox, ReSample
import fiona
from fiona.crs import from_epsg

tempfile_name = "temp_shape.shp"

def shape_array_to_shapefile(data, fname):
    if data[0].shape[0] == 2:
        type = 'Polygon'
    else:
        type = 'MultiPolygon'

    epsg = from_epsg(data[1])
    driver = "ESRI Shapefile"
    polygon = {
        'geometry': {
            'type': type,
            'coordinates': data[0]
        },
        'properties': {
            'name': 'TempCroppingPolygon'
        } # Do we need any other metadata for a tempfile?
    }
    schema = {
        'geometry': type,
        'properties': {
            'name': 'str'
        }
    }
    with fiona.open(fname, 'w', crs=epsg, driver=driver, schema=schema) as shapefile:
        shapefile.write(polygon)


def get_namespaces(sm: ArgType.DataSet):
    mint_ns = sm.ns("https://mint.isi.edu/")
    mint_geo_ns = sm.ns("https://mint.isi.edu/geo")
    rdf_ns = sm.ns(outputs.Namespace.RDF)
    return mint_ns, mint_geo_ns, rdf_ns

def extract_raster(sm: ArgType.DataSet, variable_name: str):
    mint_ns, mint_geo_ns, rdf_ns = get_namespaces(sm)

    rasters = []
    for c in sm.c(mint_ns.Variable).filter(outputs.FCondition(mint_ns.standardName, "==", variable_name)):
        for raster_id, sc in c.group_by(mint_geo_ns.raster):
            data = sc.p(rdf_ns.value).as_ndarray([sc.p(mint_geo_ns.lat), sc.p(mint_geo_ns.long)])
            gt_info = sm.get_record_by_id(raster_id)

            if data.index_props[0].size > 1 and data.index_props[0][1] > data.index_props[0][0]:
                data.data = data.data[::-1]
                data.index_props[0] = data.index_props[0][::-1]
            gt = GeoTransform(x_min=gt_info.s(mint_geo_ns.x_min),
                              y_max=gt_info.s(mint_geo_ns.y_min) + gt_info.s(mint_geo_ns.dy) * data.data.shape[0],
                              dx=gt_info.s(mint_geo_ns.dx), dy=-gt_info.s(mint_geo_ns.dy))

            rasters.append(Raster(data.data, gt, int(gt_info.s(mint_geo_ns.epsg))))

    return rasters

def extract_shape(sm: ArgType.DataSet):
    mint_ns, mint_geo_ns, rdf_ns = get_namespaces(sm)

    shapes = []
    for c in sm.c(mint_ns.Place):
        for r in c.iter_records():
            polygon = sm.get_record_by_id(r.s(mint_geo_ns.bounding)).s(rdf_ns.value)
            # epsg = int(record.s(mint_geo_ns.epsg))
            epsg = 4326

            shapes.append([polygon, epsg])

    return shapes

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

    outputs = {"array": ArgType.DataSet}

    def __init__(
        self, variable_name: str, dataset, shape, xmin: int, ymin: int, xmax: int, ymax: int
    ):
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


    def _crop_boundbox(self):
        self.rasters = extract_raster(self.dataset, self.variable_name)
        bb = BoundingBox(x_min=self.xmin, y_min=self.ymin, x_max=self.xmax, y_max=self.ymax)

        self.results = []
        for r in self.rasters:
            cropped_raster = r.crop(bounds=bb)
            self.results.append(cropped_raster.data)

    def _crop_shape_dataset(self):
        self.rasters = extract_raster(self.dataset, self.variable_name)
        self.shapes = extract_shape(self.shape_sm)

        self.results = []
        for r in self.rasters:
            for s in self.shapes:
                shape_array_to_shapefile(s, tempfile_name)
                cropped_raster= r.crop(vector_file=tempfile_name)

                self.results.append(cropped_raster.data)

    def crop_shape_sharedbackend(self):
        pass

    def exec(self):
        if self.use_bbox:
            self._crop_boundbox()
        else:
            self._crop_shape_dataset()

        return array(self.results)
