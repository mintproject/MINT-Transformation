from dtran.dcat.api import DCatAPI
from funcs.readers.dcat_read_func import DATA_CATALOG_DOWNLOAD_DIR
import os
import math
from pathlib import Path
from typing import Optional, Dict

import re
import json

import pandas as pd
import xarray as xr

from dtran import IFunc, ArgType
from dtran.ifunc import IFuncType
from dtran.metadata import Metadata

import logging, sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

class SelectGldasCoordinates(IFunc):
    id = "cycles_select_gldas_coordinates"
    description = """ A reader-transformation-writer multi-adapter.
    Select nearest gldas coordinates for soil files. Creates multiple output files for further processing (gldas_to_cycles)
    """
    inputs = {
        "soil_dataset_id": ArgType.String,
        "gldas_elevation_file_path": ArgType.String,
        "bounding_box": ArgType.String,
        "num_output_files": ArgType.Number,
        "output_path": ArgType.FilePath,
        "output_prefix": ArgType.String
    }
    outputs = {"output_files": ArgType.FilePath}
    friendly_name: str = "Gldas2CyclesNew"
    func_type = IFuncType.MODEL_TRANS
    example = {
        "soil_dataset_id": "ac34f01b-1484-4403-98ea-3a380838cab1",
        "gldas_elevation_file_path": "/tmp/GLDASp4_elevation_025d.nc4",
        "bounding_box": "21.533203125, -5.353521355337321, 51.943359375, 22.67484735118852",
        "num_output_files": 40,
        "output_path": "/tmp/output",
        "output_prefix": "output_prefix"
    }

    def __init__(
        self,
        soil_dataset_id,
        gldas_elevation_file_path,
        bounding_box,
        num_output_files,
        output_path,
        output_prefix
    ):
        self.soil_dataset_id = soil_dataset_id
        self.gldas_elevation_file_path = gldas_elevation_file_path
        self.output_path = output_path
        self.output_prefix = output_prefix
        self.bounding_box = bounding_box
        self.num_output_files = num_output_files

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        output_file = select_gldas_coordinates(
            self.soil_dataset_id,
            self.gldas_elevation_file_path,
            self.bounding_box,
            self.num_output_files,            
            self.output_path,
            self.output_prefix
        )
        return {"output_files": output_file}

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata


def get_geometry(bboxstr):
    if bboxstr is None:
        return None
    coords = re.split(r"\s*,\s*", bboxstr)
    if coords is None or len(coords) != 4:
        return None
    x1 = float(coords[0])
    y1 = float(coords[1])
    x2 = float(coords[2])
    y2 = float(coords[3])
    return {
        "type": "Polygon",
        "coordinates": [[[ x1, y1 ], [ x2, y1 ], [ x2, y2 ], [ x1, y2 ], [ x1, y1 ]]]
    }

    
def load_gldas_elevation_dataset(gldas_elevation_file):
    """
    Load GLDAS elevation dataset using XArray
    """       
    d1 = xr.open_dataset(gldas_elevation_file).load()
    return d1

def split_into_chunks(full_list, numchunks):
    chunksize = math.ceil(len(full_list)/numchunks)
    if chunksize == 0:
        return []
    for i in range(0, len(full_list), chunksize):
        yield full_list[i:i + chunksize]

def select_gldas_coordinates(
    soil_dataset_id,
    gldas_elevation_file_path,
    bounding_box,
    num_output_files,
    output_path,
    output_prefix
):
    soil_directory = DATA_CATALOG_DOWNLOAD_DIR + "/soil"
    if not os.path.exists(soil_directory):
       Path(soil_directory).mkdir(exist_ok=True, parents=True)

    if not os.path.exists(output_path):
       Path(output_path).mkdir(exist_ok=True, parents=True)

    # Download Soil Datasets & Get their Lat/Long
    geometry = get_geometry(bounding_box)
    soil_resources = DCatAPI.get_instance().find_resources_by_dataset_id(soil_dataset_id, geometry=geometry)

    logging.info("Downloading missing soil data..")
    coords = []
    cnt = 0
    for resource in soil_resources:
        meta = resource['resource_metadata']
        spatial_metadata = meta['spatial_coverage']
        if spatial_metadata['type'] == 'Point':
            cnt+=1
            lat = float(spatial_metadata['value']['y'])
            lon = float(spatial_metadata['value']['x'])
            meta['resource_type'] = '.zip'
            ofile = os.path.join(soil_directory, resource['resource_name'])
            if not os.path.exists(ofile):
                logging.debug(ofile)
                #FIXME: subprocess.check_call(f"wget -q \"{resource['resource_data_url']}\" -O {ofile}", shell=True, close_fds=False)
            coords.append((lat, lon, ofile, "%s-%.5f-%.5f" % (output_prefix, lat, lon)))

    logging.info("Loading GLDAS elevation data")
    gldas_elevation_ds = load_gldas_elevation_dataset(gldas_elevation_file_path)
    logging.info("GLDAS Elevation data loaded")

    num_soil_points = len(coords)
    logging.info(f"Fetching nearest GLDAS grid points for {num_soil_points} Soil points...")

    weather_points = {}
    for lat, lon, soil_path, fname in coords:
        logging.debug("Soil point: {0}, {1}".format(lat, lon))

        elevation = float("nan")
        cur_lat = lat
        cur_lon = lon
        cur_mul = -1
        while pd.isna(elevation):
            # Get closest GLDAS Grid point from Elevation Dataset
            loc_elevation_ds = gldas_elevation_ds.sel(lat=cur_lat, lon=cur_lon, method="nearest")
            elevation = float(loc_elevation_ds.GLDAS_elevation.values[0])
            cur_lon += cur_mul*0.25
            cur_mul = cur_mul*-1.5

        # Get the Grid point location and elevation
        grid_lat = float(loc_elevation_ds.lat.values)
        grid_lon = float(loc_elevation_ds.lon.values)

        if grid_lat < 0.0:
            lat_str = "%.2fS" % (abs(grid_lat))
        else:
            lat_str = "%.2fN" % (abs(grid_lat))

        if grid_lon < 0.0:
            lon_str = "%.2fW" % (abs(grid_lon))
        else:
            lon_str = "%.2fE" % (abs(grid_lon))    
        
        weather_fname = f"cycles_weather_{lat_str}_{lon_str}.weather"
        
        # Use memoize to make sure that the same weather file isn't generated for different soil points
        if (weather_fname) not in weather_points:
            weather_points[weather_fname] = {
                "weather": {
                    "filename": weather_fname,
                    "lat": grid_lat,
                    "lon": grid_lon,
                    "elevation": elevation
                },
                "soils": []
            }
        soil = {
            "name": fname,
            "path": soil_path
        }
        weather_points[weather_fname]["soils"].append(soil)
        
        logging.debug(f"Closest grid point location: {grid_lat},{grid_lon}, elevation: {elevation}")

    gldas_elevation_ds.close()

    point_chunks = split_into_chunks(list(weather_points.values()), num_output_files)
    i=1
    for point_chunk in point_chunks:
        outputfile = f"{output_path}/{output_prefix}_soilmap_{i}.json"
        with open(outputfile, "w") as chunkfd:
            json.dump(point_chunk, chunkfd, indent=3)
        i += 1

    return weather_points.keys
