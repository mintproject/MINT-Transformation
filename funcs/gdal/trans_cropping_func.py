#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import glob
import uuid
import fiona
from fiona.crs import from_epsg
from typing import Optional, Dict

from dtran.argtype import ArgType
from dtran.backend import ShardedBackend
from dtran.ifunc import IFunc, IFuncType
from dtran.metadata import Metadata
from drepr import outputs
from drepr.executors.readers.reader_container import ReaderContainer
from funcs.gdal.raster import Raster, GeoTransform, BoundingBox, ReSample
from funcs.gdal.raster_to_dataset import raster_to_dataset


class CroppingTransFunc(IFunc):
    id = "cropping_trans"
    description = ""

    inputs = {
        "dataset": ArgType.DataSet(None),
        "variable_name": ArgType.String(optional=True),
        "shape": ArgType.DataSet(None, optional=True),
        "xmin": ArgType.Number(optional=True),
        "ymin": ArgType.Number(optional=True),
        "xmax": ArgType.Number(optional=True),
        "ymax": ArgType.Number(optional=True),
        "region_label": ArgType.String(optional=True)
        # TODO When implementing validator, make sure bounding box inputs always has region label
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
        "region_label": ""
    }

    def __init__(self, dataset, variable_name: str = "", shape=None, xmin=0, ymin=0, xmax=0, ymax=0, region_label=""):
        self.variable_name = variable_name
        self.dataset = dataset
        self.shape_sm = shape
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
        self.region_label = region_label

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
                crs=epsg,
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

        classes = sm.c(mint_ns.Variable)
        if variable_name:
            classes = classes.filter(outputs.FCondition(mint_ns.standardName, "==", variable_name))

        for c in classes:
            for raster_id, sc in c.group_by(mint_geo_ns.raster):
                var_record = next(sc.iter_records())
                var_name = variable_name  # TODO Case where no variable name is given and none exists in schema?
                if not variable_name:
                    # Extract variable name if we did not filter
                    if sc.p(mint_ns.standardName) is not None:
                        var_name = var_record.s(mint_ns.standardName)

                if sc.p(mint_ns.timestamp) is not None:
                    timestamp = var_record.s(mint_ns.timestamp)
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
                    data.nodata.value.item() if data.nodata is not None else None,
                )
                rasters.append({"raster": raster, "timestamp": timestamp, "variable_name": var_name})

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

        results = []
        for r in self.rasters:
            cropped_raster = r["raster"].crop(bounds=bb, resampling_algo=ReSample.BILINEAR)
            if cropped_raster is None:
                continue
            results.append(raster_to_dataset(cropped_raster, r["variable_name"], timestamp=r["timestamp"], region_label=self.region_label))
        assert len(results) > 0, "No overlapping data for the given region"
        self.results = ShardedBackend(len(results))
        for result, temp_file in results:
            self.results.add(result(self.results.inject_class_id))
            ReaderContainer.get_instance().delete(temp_file)

    def _crop_shape_dataset(self):
        self.rasters = CroppingTransFunc.extract_raster(self.dataset, self.variable_name)
        self.shapes = CroppingTransFunc.extract_shape(self.shape_sm)

        results = []
        for r in self.rasters:
            for shape in self.shapes:
                tempfile_name = f"/tmp/{uuid.uuid4()}.shp"
                CroppingTransFunc.shape_array_to_shapefile(shape, tempfile_name)
                cropped_raster = r["raster"].crop(
                    vector_file=tempfile_name, resampling_algo=ReSample.BILINEAR, touch_cutline=True
                )
                for f in glob.glob(f"{tempfile_name[:-4]}*"):
                    os.remove(f)
                if cropped_raster is None:
                    continue
                place = shape['place']
                results.append(raster_to_dataset(cropped_raster, r["variable_name"], place=place, timestamp=r["timestamp"]))
        assert len(results) > 0, "No overlapping data for the given region"
        self.results = ShardedBackend(len(results))
        for result, temp_file in results:
            self.results.add(result(self.results.inject_class_id))
            ReaderContainer.get_instance().delete(temp_file)

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

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata
