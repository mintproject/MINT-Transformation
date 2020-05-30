from typing import *
from funcs.gdal.raster import *
from drepr import DRepr, outputs
from drepr.executors.readers.reader_container import ReaderContainer
from drepr.executors.readers.np_dict import NPDictReader
import copy
import uuid
from functools import partial
from urllib.parse import urlencode

raster_model = {
    "version": "2",
    "resources": "container",
    "attributes": {
        "variable_name": "$.variable_name",
        "variable": {
            "path": "$.variable[:][:]",
            "missing_values": []
        },
        "lat": "$.lat[:]",
        "long": "$.long[:]",
        "timestamp": "$.timestamp",
        "gt_x_0": "$.gt_x_0",
        "gt_y_0": "$.gt_y_0",
        "gt_dx": "$.gt_dx",
        "gt_dy": "$.gt_dy",
        "gt_epsg": "$.gt_epsg",
        "gt_x_slope": "$.gt_x_slope",
        "gt_y_slope": "$.gt_y_slope",
    },
    "alignments": [
                      {"type": "dimension", "value": "variable:1 <-> lat:1"},
                      {"type": "dimension", "value": "variable:2 <-> long:1"}
                  ] +
                  [
                      {"type": "dimension", "source": "variable", "target": x, "aligned_dims": []}
                      for x in
                      ["variable_name", "timestamp", "gt_x_0", "gt_y_0", "gt_dx", "gt_dy", "gt_epsg", "gt_x_slope",
                       "gt_y_slope"]
                  ],
    "semantic_model": {
        "mint:Variable:1": {
            "properties": [
                ("rdf:value", "variable"),
                ("mint-geo:lat", "lat"),
                ("mint-geo:long", "long"),
                ("mint:timestamp", "timestamp"),
                ("mint:standardName", "variable_name")
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
        "prefixes": {
            "mint": "https://mint.isi.edu/",
            "mint-geo": "https://mint.isi.edu/geo"
        }
    }
}


def _update_model(place_dict, place_parameters):
    updated_model = copy.deepcopy(raster_model)
    updated_place_parameters = [pp for pp in place_parameters if f"mint:{pp}" in place_dict]

    updated_model['attributes'].update({
        f"place_{pp}": f"$.place_{pp}" for pp in updated_place_parameters
    })
    updated_model['attributes']["place_uri"] = "$.place_uri"
    updated_model['alignments'].extend(
        {"type": "dimension", "source": "variable", "target": f"place_{pp}", "aligned_dims": []}
        for pp in updated_place_parameters
    )
    updated_model['alignments'].append({"type": "dimension", "source": "variable", "target": "place_uri", "aligned_dims": []})
    updated_model['semantic_model']["mint:Place:1"] = {
        "properties": [
            (f"mint:{pp}", f"place_{pp}")
            for pp in updated_place_parameters
        ] + [("drepr:uri", "place_uri")]
    }

    return updated_model


def raster_to_dataset(raster: Raster, variable_name: str, place=None, region_label="", timestamp=None):
    place_parameters = ['region', 'zone', 'district']
    used_region_label = False
    if place:
        place_dict = place.to_dict()
    else:
        assert region_label
        used_region_label = True
        place_dict = {"mint:region": [region_label]}

    model = _update_model(place_dict, place_parameters)
    if isinstance(raster.nodata, (float, np.float32, np.float64)):
        model['attributes']['variable']['missing_values'].append(float(raster.nodata))
    elif isinstance(raster.nodata, (int, np.int32, np.int64)):
        model['attributes']['variable']['missing_values'].append(int(raster.nodata))
    else:
        model['attributes']['variable']['missing_values'].append(raster.nodata)

    dsmodel = DRepr.parse(model)
    data = {
        "variable_name": variable_name,
        "variable": raster.data,
        "timestamp": timestamp,
        "place_uri": f"https://mint.isi.edu/place_uri?{urlencode(sorted(place_dict.items()))}",
        "lat": raster.get_center_latitude(),
        "long": raster.get_center_longitude(),
        "gt_x_0": raster.geotransform.x_0,
        "gt_y_0": raster.geotransform.y_0,
        "gt_dx": raster.geotransform.dx,
        "gt_dy": raster.geotransform.dy,
        "gt_epsg": 4326,
        "gt_x_slope": raster.geotransform.x_slope,
        "gt_y_slope": raster.geotransform.y_slope
    }
    for pp in place_parameters:
        if f"mint:{pp}" in place_dict:
            if used_region_label:
                data[f"place_{pp}"] = region_label
                continue
            data[f"place_{pp}"] = place.s(f"mint:{pp}")
    reader = NPDictReader(data)
    temp_file = f"resource_{str(uuid.uuid4())}"
    ReaderContainer.get_instance().set(temp_file, reader)
    new_sm = partial(outputs.ArrayBackend.from_drepr, dsmodel, temp_file)
    return new_sm, temp_file
