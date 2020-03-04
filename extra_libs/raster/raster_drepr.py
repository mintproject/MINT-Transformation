from typing import *
from extra_libs.raster.raster import *
from drepr import DRepr, outputs
from drepr.executors.readers.reader_container import ReaderContainer
from drepr.executors.readers.np_dict import NPDictReader

drepr_model = {
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
        "gt_y_shope": "$.gt_y_shope",
    },
    "alignments": [
        {"type": "dimension", "source": "variable", "target": x, "aligned_dims": []}
        for x in ["nodata", "gt_x_0", "gt_y_0", "gt_dx", "gt_dy", "gt_epsg", "gt_x_slope", "gt_y_shope"]
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
                ("mint-geo:y_shope", "gt_y_shope"),
            ]
        },
        "prefixes": {
            "mint": "https://mint.isi.edu/",
            "mint-geo": "https://mint.isi.edu/geo"
        }
    }
}

def rasters_to_datasets(rasters: List[Raster]):
    dsmodel = DRepr.parse(drepr_model)
    datasets = []
    for r in rasters:
        reader = NPDictReader({
            "variable": r.data,
            "nodata": r.nodata,
            "gt_x_0": r.geotransform.x_0,
            "gt_y_0": r.geotransform.y_0,
            "gt_dx": r.geotransform.dx,
            "gt_dy": r.geotransform.dy,
            "gt_epsg": 4326,
            "gt_x_slope": r.geotransform.x_slope,
            "gt_y_shope": r.geotransform.y_slope
        })
        temp_file = f"resource_temp"
        ReaderContainer.get_instance().set(temp_file, reader)
        new_sm = outputs.ArrayBackend.from_drepr(dsmodel, temp_file)
        datasets.append(new_sm)

    return datasets
