from typing import *
from extra_libs.raster.raster import *
from drepr import DRepr, outputs
from drepr.executors.readers.reader_container import ReaderContainer
from drepr.executors.readers.np_dict import NPDictReader

without_place_model = {
    "version": "2",
    "resources": "container",
    "attributes": {
        "variable": "$.variable[:][:]",
        "nodata": "$.nodata",
        "gt_x_0": "$.gt_x_0",
        "gt_y_0": "$.gt_y_0",
        "gt_dx": "$.gt_dx",
        "gt_dy": "$.gt_dy",
        "gt_epsg": "$.gt_epsg",
        "gt_x_slope": "$.gt_x_slope",
        "gt_y_slope": "$.gt_y_slope",
    },
    "alignments": [
        {"type": "dimension", "source": "variable", "target": x, "aligned_dims": []}
        for x in ["nodata", "gt_x_0", "gt_y_0", "gt_dx", "gt_dy", "gt_epsg", "gt_x_slope", "gt_y_slope"]
    ],
    "semantic_model": {
        "mint:Variable:1": {
            "properties": [
                ("rdf:value", "variable")
            ],
            "links": [
                ("mint-geo:raster", "mint-geo:Raster:1")
            ]
        },
        "mint-geo:Raster:1": {
            "properties": [
                ("mint-geo:x_0", "gt_x_0"),
                ("mint-geo:y_0", "gt_y_0"),
                ("mint-geo:dx", "gt_dx"),
                ("mint-geo:dy", "gt_dy"),
                ("mint-geo:epsg", "gt_epsg"),
                ("mint-geo:x_slope", "gt_x_slope"),
                ("mint-geo:y_slope", "gt_y_slope"),
            ]
        },
        "prefixes": {
            "mint": "https://mint.isi.edu/",
            "mint-geo": "https://mint.isi.edu/geo"
        }
    }
}

with_place_model = {
    "version": "2",
    "resources": "container",
    "attributes": {
        "variable": "$.variable[:][:]",
        "nodata": "$.nodata",
        "gt_x_0": "$.gt_x_0",
        "gt_y_0": "$.gt_y_0",
        "gt_dx": "$.gt_dx",
        "gt_dy": "$.gt_dy",
        "gt_epsg": "$.gt_epsg",
        "gt_x_slope": "$.gt_x_slope",
        "gt_y_slope": "$.gt_y_slope",
        "place_region": "$.place_region",
        "place_zone": "$.place_zone",
        "place_district": "$.place_district",
    },
    "alignments": [
        {"type": "dimension", "source": "variable", "target": x, "aligned_dims": []}
        for x in ["nodata", "gt_x_0", "gt_y_0", "gt_dx", "gt_dy", "gt_epsg", "gt_x_slope", "gt_y_slope", "place_region",
                  "place_zone", "place_district"]
    ],
    "semantic_model": {
        "mint:Variable:1": {
            "properties": [
                ("rdf:value", "variable")
            ],
            "links": [
                ("mint:place", "mint:Place:1"),
                ("mint-geo:raster", "mint-geo:Raster:1")
            ]
        },
        "mint-geo:Raster:1": {
            "properties": [
                ("mint-geo:x_0", "gt_x_0"),
                ("mint-geo:y_0", "gt_y_0"),
                ("mint-geo:dx", "gt_dx"),
                ("mint-geo:dy", "gt_dy"),
                ("mint-geo:epsg", "gt_epsg"),
                ("mint-geo:x_slope", "gt_x_slope"),
                ("mint-geo:y_slope", "gt_y_slope"),
            ]
        },
        "mint:Place:1": {
            "properties": [
                ("mint:region", "place_region"),
                ("mint:zone", "place_zone"),
                ("mint:district", "place_district"),
            ]
        },
        "prefixes": {
            "mint": "https://mint.isi.edu/",
            "mint-geo": "https://mint.isi.edu/geo"
        }
    }
}


def raster_to_dataset(raster: Raster, place=None):
    model = without_place_model
    if place:
        model = with_place_model
    dsmodel = DRepr.parse(model)
    alignment = {
        "variable": raster.data,
        "nodata": raster.nodata,
        "gt_x_0": raster.geotransform.x_0,
        "gt_y_0": raster.geotransform.y_0,
        "gt_dx": raster.geotransform.dx,
        "gt_dy": raster.geotransform.dy,
        "gt_epsg": 4326,
        "gt_x_slope": raster.geotransform.x_slope,
        "gt_y_slope": raster.geotransform.y_slope
    }
    if place:
        alignment.update({
            "place_region": place.s("mint:region"),
            "place_zone": place.s("mint:zone"),
            "place_district": place.s("mint:district")
        })
    reader = NPDictReader(alignment)
    temp_file = f"resource_temp"
    ReaderContainer.get_instance().set(temp_file, reader)
    new_sm = outputs.ArrayBackend.from_drepr(dsmodel, temp_file)

    return new_sm
