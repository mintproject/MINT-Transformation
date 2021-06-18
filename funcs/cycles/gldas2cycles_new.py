import argparse
import subprocess
from dtran.dcat.api import DCatAPI
from funcs.readers.dcat_read_func import DATA_CATALOG_DOWNLOAD_DIR, ResourceManager
import math
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict

import numpy as np
from netCDF4 import Dataset
from dtran import IFunc, ArgType
from dtran.ifunc import IFuncType
from dtran.metadata import Metadata
from zipfile import ZipFile


class Gldas2CyclesFuncNew(IFunc):
    id = "topoflow4_climate_write_func"
    description = """ A reader-transformation-writer multi-adapter.
    Creates Cycles input (weather and soil file zip) from GLDAS NetCDF (climate) files & Soil files.
    """
    inputs = {
        "gldas_dataset_id": ArgType.String,
        "soil_dataset_id": ArgType.String,
        "start_date": ArgType.String,
        "end_date": ArgType.String,
        "gldas_path": ArgType.FilePath,
        "output_path": ArgType.FilePath,
        "output_prefix": ArgType.String
    }
    outputs = {"output_files": ArgType.FilePath}
    friendly_name: str = "Gldas2CyclesNew"
    func_type = IFuncType.MODEL_TRANS
    example = {
        "gldas_dataset_id": "5babae3f-c468-4e01-862e-8b201468e3b5",
        "soil_dataset_id": "ac34f01b-1484-4403-98ea-3a380838cab1",
        "start_date": "2000-01-01",
        "end_date": "2018-01-31",
        "gldas_path": "/tmp/input/gldas",
        "output_path": "/tmp/output",
        "output_prefix": "output_prefix"
    }

    def __init__(
        self,
        gldas_dataset_id,
        soil_dataset_id,
        start_date,
        end_date,
        gldas_path,
        output_path,
        output_prefix
    ):
        self.gldas_dataset_id = gldas_dataset_id
        self.soil_dataset_id = soil_dataset_id
        self.output_path = output_path
        self.output_prefix = output_prefix
        self.gldas_path = gldas_path
        self.end_date = end_date
        self.start_date = start_date

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        output_file = gldas2cycles(
            self.gldas_dataset_id,
            self.soil_dataset_id,
            self.start_date,
            self.end_date,
            self.gldas_path,
            self.output_path,
            self.output_prefix
        )
        return {"output_files": output_file}

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata


def Closest(lat, lon, path):

    elevation_fp = path + "/GLDASp4_elevation_025d.nc4"
    nc = Dataset(elevation_fp, "r")

    best_y = (np.abs(nc.variables["lat"][:] - lat)).argmin()
    best_x = (np.abs(nc.variables["lon"][:] - lon)).argmin()

    return (
        best_y,
        best_x,
        nc["lat"][best_y],
        nc["lon"][best_x],
        nc["GLDAS_elevation"][0, best_y, best_x],
    )


def ReadVar(y, x, nc_name):
    with Dataset(nc_name, "r") as nc:
        _prcp = nc["Rainf_f_tavg"][0, y, x]
        _temp = nc["Tair_f_inst"][0, y, x]
        _wind = nc["Wind_f_inst"][0, y, x]
        _solar = nc["SWdown_f_tavg"][0, y, x]
        _pres = nc["Psurf_f_inst"][0, y, x]
        _spfh = nc["Qair_f_inst"][0, y, x]

        es = 611.2 * math.exp(17.67 * (_temp - 273.15) / (_temp - 273.15 + 243.5))
        ws = 0.622 * es / (_pres - es)
        w = _spfh / (1.0 - _spfh)
        _rh = w / ws
        if _rh > 1.0:
            _rh = 1.0

        return _prcp, _temp, _wind, _solar, _rh


def satvp(temp):
    return 0.6108 * math.exp(17.27 * temp / (temp + 237.3))


def ea(patm, q):
    return patm * q / (0.622 * (1 - q) + q)


def process_day(t, y, x, path):
    """
    process one day of GLDAS data and convert it to Cycles input
    """

    prcp = 0.0
    tx = -999.0
    tn = 999.0
    wind = 0.0
    solar = 0.0
    rhx = -999.0
    rhn = 999.0
    counter = 0

    #print(datetime.strftime(t, "%Y-%m-%d"))

    nc_path = "%s/%4.4d/%3.3d/" % (path, t.timetuple().tm_year, t.timetuple().tm_yday)

    for nc_name in os.listdir(nc_path):
        if nc_name.endswith(".nc4"):
            (_prcp, _temp, _wind, _solar, _rh) = ReadVar(
                y, x, os.path.join(nc_path, nc_name)
            )

            prcp += _prcp

            if _temp > tx:
                tx = _temp

            if _temp < tn:
                tn = _temp

            wind += _wind

            solar += _solar

            if _rh > rhx:
                rhx = _rh

            if _rh < rhn:
                rhn = _rh

            counter += 1

    prcp /= float(counter)
    prcp *= 86400.0

    wind /= float(counter)

    solar /= float(counter)
    solar *= 86400.0 / 1.0e6

    rhx *= 100.0
    rhn *= 100.0

    tx -= 273.15
    tn -= 273.15

    data = "%-16s%-8.4f%-8.2f%-8.2f%-8.4f%-8.2f%-8.2f%-8.2f\n" % (
        t.strftime("%Y    %j"),
        prcp,
        tx,
        tn,
        solar,
        rhx,
        rhn,
        wind,
    )

    return data


def gldas2cycles(
    gldas_dataset_id,
    soil_dataset_id,
    start_date,
    end_date,
    gldas_path,
    output_path,
    output_prefix
):

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    soil_directory = DATA_CATALOG_DOWNLOAD_DIR + "/soil"
    gldas_directory = DATA_CATALOG_DOWNLOAD_DIR + "/gldas"
    Path(soil_directory).mkdir(exist_ok=True, parents=True)
    Path(gldas_directory).mkdir(exist_ok=True, parents=True)

    # Download Soil Datasets & Get their Lat/Long
    soil_resources = DCatAPI.get_instance().find_resources_by_dataset_id(soil_dataset_id)

    print ("Downloading missing soil data..")
    
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
            ofile = os.path.join(soil_directory, resource['resource_id'])
            if not os.path.exists(ofile):
                print(ofile)
                subprocess.check_call(f"wget -q \"{resource['resource_data_url']}\" -O {ofile}", shell=True, close_fds=False)
            coords.append((lat, lon, ofile, "%s-%.5f-%.5f" % (output_prefix, lat, lon)))

    print("Downloading missing gldas data..")

    # Download GLDAS Datasets for the time period
    gldas_resources = DCatAPI.get_instance().find_resources_by_dataset_id(gldas_dataset_id, start_date, end_date)    
    for resource in gldas_resources:
        temporal_metadata = resource['resource_metadata']['temporal_coverage']
        gldas_date_str = temporal_metadata['start_time'].split("T")[0]
        gldas_date = datetime.strptime(gldas_date_str, "%Y-%m-%d")
        nc_path = "%s/%4.4d/%3.3d/" % (gldas_directory, gldas_date.timetuple().tm_year, gldas_date.timetuple().tm_yday)
        ofile = os.path.join(nc_path, resource['resource_name'])
        if not os.path.exists(nc_path):
            Path(nc_path).mkdir(parents=True, exist_ok=True)
        if not os.path.exists(ofile):
            print(ofile)
            subprocess.check_call(f"wget -q \"{resource['resource_data_url']}\" -O {ofile}", shell=True, close_fds=False)

    memoize = {}
    fnames = []
    for lat, lon, soil_path, fname in coords:
        print("Processing data for {0}, {1}".format(lat, lon))

        (y, x, grid_lat, grid_lon, elevation) = Closest(lat, lon, gldas_directory)

        if grid_lat < 0.0:
            lat_str = "%.2fS" % (abs(grid_lat))
        else:
            lat_str = "%.2fN" % (abs(grid_lat))

        if grid_lon < 0.0:
            lon_str = "%.2fW" % (abs(grid_lon))
        else:
            lon_str = "%.2fE" % (abs(grid_lon))
        
        weather_fname = fname + ".weather"
        soil_fname = fname + ".soil"
        zip_fname = fname + "_soil_weather.zip"

        tmp_soil_file = os.path.join(output_path, soil_fname)        
        tmp_weather_file = os.path.join(output_path, weather_fname)        
        soil_weather_file = os.path.join(output_path, zip_fname)

        if (lat_str, lon_str) in memoize:
            shutil.copyfile(memoize[(lat_str, lon_str)], tmp_weather_file)
            continue
        else:
            memoize[(lat_str, lon_str)] = tmp_weather_file

        Path(output_path).mkdir(parents=True, exist_ok=True)

        # fname = "met" + lat_str + "x" + lon_str + ".weather"
        outfp = open(tmp_weather_file, "w")
        outfp.write("LATITUDE %.2f\n" % (grid_lat))
        outfp.write("LONGITUDE %.2f\n" % (grid_lon))
        outfp.write("ALTITUDE %.2f\n" % (elevation))
        outfp.write("SCREENING_HEIGHT 2\n")
        outfp.write(
            "YEAR    DOY     PP      TX      TN     SOLAR      RHX      RHN     WIND\n"
        )

        cday = start_date

        while cday < end_date:
            outfp.write(process_day(cday, y, x, gldas_directory))
            cday += timedelta(days=1)

        outfp.close()
        shutil.copyfile(soil_path, Path(tmp_soil_file))

        zipObj = ZipFile(soil_weather_file, 'w')
        zipObj.write(tmp_soil_file, soil_fname)
        zipObj.write(tmp_weather_file, weather_fname)
        zipObj.close()
        
        fnames.append(zip_fname)
    return fnames
