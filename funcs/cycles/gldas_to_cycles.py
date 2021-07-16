import argparse
import subprocess
from dtran.dcat.api import DCatAPI
from funcs.readers.dcat_read_func import DATA_CATALOG_DOWNLOAD_DIR
import os
import csv
import json
import shutil
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Optional, Dict

import re

import xarray as xr
from netCDF4 import Dataset
from dtran import IFunc, ArgType
from dtran.ifunc import IFuncType
from dtran.metadata import Metadata
from zipfile import ZipFile

import logging, sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
class GldasToCyclesBatched(IFunc):
    id = "gldas_to_cycles_batched_func"
    description = """ A reader-transformation-writer multi-adapter.
    Creates Cycles input (weather and soil file zip) from GLDAS NetCDF (climate) files & Soil files.
    """
    inputs = {
        "gldas_dataset_id": ArgType.String,
        "gldas_soil_map_file": ArgType.String,
        "start_date": ArgType.String,
        "end_date": ArgType.String,
        "batch_numdays": ArgType.Number,
        "output_path": ArgType.FilePath
    }
    outputs = {"output_files": ArgType.FilePath}
    friendly_name: str = "GldasToCyclesBatched"
    func_type = IFuncType.MODEL_TRANS
    example = {
        "gldas_dataset_id": "5babae3f-c468-4e01-862e-8b201468e3b5",
        "gldas_soil_map_file": "/tmp/gldas_soil_43.E_8.4N.json",
        "start_date": "2000-01-01",
        "end_date": "2018-01-31",
        "batch_numdays": 14,
        "output_path": "/tmp/output"
    }

    def __init__(
        self,
        gldas_dataset_id,
        gldas_soil_map_file,
        start_date,
        end_date,
        batch_numdays,
        output_path
    ):
        self.gldas_dataset_id = gldas_dataset_id
        self.gldas_soil_map_file = gldas_soil_map_file
        self.output_path = output_path
        self.end_date = end_date
        self.start_date = start_date
        self.batch_numdays = batch_numdays

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        output_file = gldas_to_cycles(
            self.gldas_dataset_id,
            self.gldas_soil_map_file,
            self.start_date,
            self.end_date,
            self.batch_numdays,
            self.output_path
        )
        return {"output_files": output_file}

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata


def convert_to_cycles_input(ds):
    """
    Resample GLDAS data for a location by 24 hours(1day), and convert to Cycles input
    """

    # Calculate RH variable values
    logging.debug("Reading variables from dataset..")
    (_prcp, _temp, _wind, _solar, _rh) = read_variables_from_dataset(ds)
    logging.debug("Finished reading variables from dataset..")

    logging.debug("Start resampling...")
    
    # Try group_by (time.dayofyear)
    # - dataarray
    # Resample/Group by 1 Day - Some variables are grouped by averaging, others by max/min
    prcp_daily = _prcp.resample(time="1D")
    temp_daily = _temp.resample(time="1D")
    solar_daily = _solar.resample(time="1D")
    rh_daily = _rh.resample(time="1D")
    wind_daily = _wind.resample(time="1D")

    prcp = prcp_daily.mean().rename("PP")
    tx = temp_daily.max().rename("TX")
    tn = temp_daily.min().rename("TN")
    solar = solar_daily.mean().rename("SOLAR")
    rhx = rh_daily.max().rename("RHX")
    rhn = rh_daily.min().rename("RHN")
    wind = wind_daily.mean().rename("WIND")

    logging.debug("Finished resampling...")

    logging.debug("Doing unit conversions...")

    # Some unit conversions
    prcp *= 86400.0
    solar *= 86400.0 / 1.0e6

    rhx *= 100.0
    rhn *= 100.0

    tx -= 273.15
    tn -= 273.15

    logging.debug("Finished unit conversions...")
    
    # Get Year and Day of Year
    year = prcp.time.dt.year.rename("YEAR")
    doy = prcp.time.dt.dayofyear.rename("DOY")

    logging.debug("Merge variables...")
    # Create a dataset with all the required variables
    cycles_weather_ds = xr.merge([year, doy, prcp, tx, tn, solar, rhx, rhn, wind])    
    cycles_weather_ds = cycles_weather_ds.reset_coords(names=["lat", "lon"], drop=True)

    logging.debug("Finished merging variables...")
    return cycles_weather_ds


def create_rh(nc):
    """
    Calculate RH (Relative Humidity) value from GLDAS data
    """
    _temp = nc["Tair_f_inst"]
    _pres = nc["Psurf_f_inst"]
    _spfh = nc["Qair_f_inst"]
    es = 611.2 * xr.ufuncs.exp(17.67 * (_temp - 273.15) / (_temp - 273.15 + 243.5))
    ws = 0.622 * es / (_pres - es)
    w = _spfh / (1.0 - _spfh)
    nc["rh"] = w / ws
    nc["rh"].clip(max=1.0)
    return nc

def read_variables_from_dataset(nc):
    """
    Read/Calculate relevant variables from GLDAS dataset
    """   
    _prcp = nc["Rainf_f_tavg"]
    _temp = nc["Tair_f_inst"]
    _wind = nc["Wind_f_inst"]
    _solar = nc["SWdown_f_tavg"]

    create_rh(nc)        
    _rh = nc["rh"]

    return _prcp, _temp, _wind, _solar, _rh


def load_gldas_dataset(gldas_files):
    """
    Load GLDAS files using XArray
    """       
    if gldas_files is not None and len(gldas_files) > 0:
        # Open a sample gldas file and get all variables to remove from the load (to make the loading faster)
        first_file = gldas_files[0]
        d1 = xr.open_dataset(first_file)
        varnames = list(d1.data_vars.keys())
        varnames.remove('Rainf_f_tavg')
        varnames.remove('Tair_f_inst')
        varnames.remove('Wind_f_inst')
        varnames.remove('SWdown_f_tavg')
        varnames.remove('Psurf_f_inst')
        varnames.remove('Qair_f_inst') 
        d1.close()

        ds=xr.open_mfdataset(gldas_files, drop_variables=varnames, chunks='auto')
        return ds


def gldas_to_cycles(
    gldas_dataset_id,
    gldas_soil_map_file,
    start_date,
    end_date,
    batch_numdays,
    output_path
):
    gldas_directory = DATA_CATALOG_DOWNLOAD_DIR + "/gldas"
    if not os.path.exists(gldas_directory):
       Path(gldas_directory).mkdir(exist_ok=True, parents=True)

    if not os.path.exists(output_path):
       Path(output_path).mkdir(exist_ok=True, parents=True)

    # Load soil and weather information from input soil-weather map file
    soil_grid_points = {}
    weather_grid_points = []
    with open(gldas_soil_map_file) as mapf:
        weather_grid_points = json.load(mapf)
        for weather_point in weather_grid_points:
            soils = weather_point["soils"]
            for soil in soils:
                soil_grid_points[soil["name"]] = {
                    "weather": weather_point["weather"],
                    "soil_path": soil["path"]
                }

    num_weather_points = len(weather_grid_points)
    num_soil_points = len(soil_grid_points.keys())
    
    logging.info(f"Processing {num_weather_points} GLDAS grid points for {num_soil_points} Soil points")

    # Get latest dates for existing weather files
    point_latest_dates = {}
    for grid_point in weather_grid_points:
        weather_point = grid_point["weather"]
        common_weather_fname = weather_point["filename"]
        lat = weather_point["lat"]
        lon = weather_point["lon"]
        elevation = weather_point["elevation"]
        common_weather_file = os.path.join(output_path, common_weather_fname)
        # Check if the weather file already exists
        if os.path.exists(common_weather_file):
            # If yes, then get latest start date for this weather file
            with open(common_weather_file) as weatherf:
                for line in weatherf:
                    items = re.split(r"\s+", line)
                    if items[0].isnumeric():
                        point_latest_dates[common_weather_fname] = datetime.strptime("{} {}".format(items[0], items[1]), "%Y %j")
        else:
            # If not, then create the weather file headers
            outfp = open(common_weather_file, "w")
            outfp.write("LATITUDE %.2f\n" % (lat))
            #outfp.write("LONGITUDE %.2f\n" % (lon))
            outfp.write("ALTITUDE %.2f\n" % (elevation))
            outfp.write("SCREENING_HEIGHT 2\n")
            outfp.write("%-8s%-8s%-8s%-8s%-8s%-8s%-8s%-8s%-8s\n" % (
                'YEAR', 'DOY', 'PP', 'TX', 'TN', 'SOLAR', 'RHX', 'RHN', 'WIND'
            ))
            outfp.close()  

    # Do the GLDAS to cycles conversion in batches of N number of days
    # - For each batch of start-date/end-date, load GLDAS and create cycles inputs    
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    cur_start_date = start_date

    while cur_start_date < end_date:
        cur_end_date = cur_start_date + timedelta(days = batch_numdays)
        if cur_end_date > end_date:
            cur_end_date = end_date
    
        logging.info(f"Fetching GLDAS files list for dates from {cur_start_date} to {cur_end_date}")

        logging.info("Downloading missing GLDAS files..")
        # Download GLDAS Datasets for the time period
        gldas_resources = DCatAPI.get_instance().find_resources_by_dataset_id(gldas_dataset_id, cur_start_date, cur_end_date)    
        gldas_files = []
        for resource in gldas_resources:
            temporal_metadata = resource['resource_metadata']['temporal_coverage']
            gldas_date_str = temporal_metadata['start_time'].split("T")[0]
            gldas_date = datetime.strptime(gldas_date_str, "%Y-%m-%d")
            nc_path = "%s/%4.4d/%3.3d/" % (gldas_directory, gldas_date.timetuple().tm_year, gldas_date.timetuple().tm_yday)
            ofile = os.path.join(nc_path, resource['resource_name'])
            if not os.path.exists(nc_path):
                Path(nc_path).mkdir(parents=True, exist_ok=True)
            if not os.path.exists(ofile):
                logging.debug(ofile)
                subprocess.check_call(f"wget -q \"{resource['resource_data_url']}\" -O {ofile}", shell=True, close_fds=False)
            if os.path.exists(ofile):
                gldas_files.append(ofile)

        num_weather_files = len(gldas_files)
        logging.info(f"Loading GLDAS data from {num_weather_files} files..")
        gldas_ds = load_gldas_dataset(gldas_files)
        logging.info("Loaded GLDAS data")
      
        # Do the cycles conversion for all weather points
        for grid_point in weather_grid_points:
            weather_point = grid_point["weather"]
            common_weather_fname = weather_point["filename"]
            lat = weather_point["lat"]
            lon = weather_point["lon"]
            elevation = weather_point["elevation"]

            common_weather_file = os.path.join(output_path, common_weather_fname)
            point_start_date = cur_start_date
            
            if common_weather_fname in point_latest_dates:
                point_start_date = point_latest_dates[common_weather_fname] + timedelta(days=1)

            # If we've already processed this time period for this point, then don't go further
            if point_start_date > cur_end_date:
                continue

            # Load GLDAS data for the exact gridpoint location
            logging.debug(f"Loading GLDAS data for grid point {lat}, {lon}")
            loc_ds = gldas_ds.sel(lat=lat, lon=lon, time=slice(point_start_date, cur_end_date)).load()
            logging.debug("Loaded gldas data for location")

            logging.debug("Converting to Cycles input data")
            # Convert to Cycles Input
            loc_by_day_ds = convert_to_cycles_input(loc_ds) 
            logging.debug("Finished conversion to cycles input data")
            
            logging.debug("Converting weather input data to Pandas Dataframe...")
            loc_by_day_df = loc_by_day_ds.to_dataframe()
            loc_by_day_df.sort_values(by=['YEAR', 'DOY'])
            logging.debug("Finished converting to Dataframe") 

            logging.debug ("Writing the cycles weather file..")
            # Append to the weather file
            outfp = open(common_weather_file, "a")
            for index, row in loc_by_day_df.iterrows():
                if index < cur_end_date: # Sometimes an extra day is returned (for midnight file of next day. Do a check here to ignore that)
                    outfp.write("%-8.0f%-8.0f%-8.4f%-8.2f%-8.2f%-8.4f%-8.2f%-8.2f%-8.2f\n" % (
                        row['YEAR'], row['DOY'],
                        row['PP'], row['TX'], row['TN'],
                        row['SOLAR'], row['RHX'], row['RHN'],
                        row['WIND'])
                    )
            outfp.close()

        gldas_ds.close()
        cur_start_date = cur_end_date

    logging.info(f"Done converting GLDAS data to cycles input weather file for {num_weather_points} points")

    logging.info(f"Creating {num_soil_points} cycles input zip files, each containing a weather and a soil file...")
    fnames = []
    # Create the Zip file for all soil points containing the soil file and the generated weather file
    for fname in soil_grid_points.keys():
        point = soil_grid_points[fname]
        soil_path = point["soil_path"]
        weather_point = point["weather"]
        if not os.path.exists(soil_path):
            continue
        common_weather_fname = weather_point["filename"]
        common_weather_file = os.path.join(output_path, common_weather_fname)

        logging.debug (f"Creating Cycles zip file for {fname}")        

        weather_fname = fname + ".weather"
        soil_fname = fname + ".soil"
        zip_fname = fname + "_soil_weather.zip"

        tmp_soil_file = os.path.join(output_path, soil_fname)    
        tmp_weather_file = os.path.join(output_path, weather_fname)        
        common_weather_file = os.path.join(output_path, common_weather_fname) 
        soil_weather_file = os.path.join(output_path, zip_fname)

        shutil.copyfile(soil_path, Path(tmp_soil_file))
        shutil.copyfile(common_weather_file, Path(tmp_weather_file))

        zipObj = ZipFile(soil_weather_file, 'w')
        zipObj.write(tmp_soil_file, soil_fname)
        zipObj.write(tmp_weather_file, weather_fname)
        zipObj.close()
        
        logging.debug ("Done writing cycles zip file")

        fnames.append(zip_fname)

    logging.info(f"Done Creating {num_soil_points} cycles input zip files")
    return fnames
