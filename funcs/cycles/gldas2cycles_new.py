import argparse
import subprocess
from dtran.dcat.api import DCatAPI
from funcs.readers.dcat_read_func import DATA_CATALOG_DOWNLOAD_DIR
import os
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
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

class Gldas2CyclesFuncNew(IFunc):
    id = "topoflow4_climate_write_func"
    description = """ A reader-transformation-writer multi-adapter.
    Creates Cycles input (weather and soil file zip) from GLDAS NetCDF (climate) files & Soil files.
    """
    inputs = {
        "gldas_dataset_id": ArgType.String,
        "soil_dataset_id": ArgType.String,
        "gldas_elevation_file_path": ArgType.String,
        "start_date": ArgType.String,
        "end_date": ArgType.String,
        "batch_numdays": ArgType.Number,
        "bounding_box": ArgType.String,
        "output_path": ArgType.FilePath,
        "output_prefix": ArgType.String
    }
    outputs = {"output_files": ArgType.FilePath}
    friendly_name: str = "Gldas2CyclesNew"
    func_type = IFuncType.MODEL_TRANS
    example = {
        "gldas_dataset_id": "5babae3f-c468-4e01-862e-8b201468e3b5",
        "soil_dataset_id": "ac34f01b-1484-4403-98ea-3a380838cab1",
        "gldas_elevation_file_path": "/tmp/GLDASp4_elevation_025d.nc4",
        "start_date": "2000-01-01",
        "end_date": "2018-01-31",
        "batch_numdays": 14,
        "bounding_box": "21.533203125, -5.353521355337321, 51.943359375, 22.67484735118852",
        "output_path": "/tmp/output",
        "output_prefix": "output_prefix"
    }

    def __init__(
        self,
        gldas_dataset_id,
        soil_dataset_id,
        gldas_elevation_file_path,
        start_date,
        end_date,
        batch_numdays,
        bounding_box,
        output_path,
        output_prefix
    ):
        self.gldas_dataset_id = gldas_dataset_id
        self.soil_dataset_id = soil_dataset_id
        self.gldas_elevation_file_path = gldas_elevation_file_path
        self.output_path = output_path
        self.output_prefix = output_prefix
        self.bounding_box = bounding_box
        self.end_date = end_date
        self.start_date = start_date
        self.batch_numdays = batch_numdays

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        output_file = gldas2cycles(
            self.gldas_dataset_id,
            self.soil_dataset_id,
            self.gldas_elevation_file_path,
            self.start_date,
            self.end_date,
            self.batch_numdays,
            self.bounding_box,
            self.output_path,
            self.output_prefix
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
    
def load_gldas_elevation_dataset(gldas_elevation_file):
    """
    Load GLDAS elevation dataset using XArray
    """       
    d1 = xr.open_dataset(gldas_elevation_file).load()
    return d1

def gldas2cycles(
    gldas_dataset_id,
    soil_dataset_id,
    gldas_elevation_file_path,
    start_date,
    end_date,
    batch_numdays,
    bounding_box,
    output_path,
    output_prefix
):

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    soil_directory = DATA_CATALOG_DOWNLOAD_DIR + "/soil"
    gldas_directory = DATA_CATALOG_DOWNLOAD_DIR + "/gldas"
    if not os.path.exists(soil_directory):
       Path(soil_directory).mkdir(exist_ok=True, parents=True)
    if not os.path.exists(gldas_directory):
       Path(gldas_directory).mkdir(exist_ok=True, parents=True)

    # Download Soil Datasets & Get their Lat/Long
    geometry = get_geometry(bounding_box)
    soil_resources = DCatAPI.get_instance().find_resources_by_dataset_id(soil_dataset_id, 
        start_time=start_date, end_time=end_date, geometry=geometry)

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


    #####################################################################
    ##
    ## Do the GLDAS to cycles conversion in batches of N number of days
    ## - Get a mapping of soil points to grid points first
    ## - For each batch of start-date/end-date, load GLDAS and create cycles inputs
    ## - Write to the cycles weather files
    ##
    #####################################################################

    logging.info("Loading GLDAS elevation data")
    gldas_elevation_ds = load_gldas_elevation_dataset(gldas_elevation_file_path)
    logging.info("GLDAS Elevation data loaded")

    num_soil_points = len(coords)
    logging.info(f"Fetching nearest GLDAS grid points for {num_soil_points} Soil points...")
    soil_weather_grid_points = {}
    memoize = {}
    for lat, lon, soil_path, fname in coords:
        logging.debug("Soil point: {0}, {1}".format(lat, lon))

        # Get closest GLDAS Grid point from Elevation Dataset
        loc_elevation_ds = gldas_elevation_ds.sel(lat=lat, lon=lon, method="nearest")

        # Get the Grid point location and elevation
        grid_lat = loc_elevation_ds.lat.values
        grid_lon = loc_elevation_ds.lon.values
        elevation = loc_elevation_ds.GLDAS_elevation.values[0]

        if grid_lat < 0.0:
            lat_str = "%.2fS" % (abs(grid_lat))
        else:
            lat_str = "%.2fN" % (abs(grid_lat))

        if grid_lon < 0.0:
            lon_str = "%.2fW" % (abs(grid_lon))
        else:
            lon_str = "%.2fE" % (abs(grid_lon))    

        soil_weather_grid_points[soil_path] = (grid_lat, grid_lon, elevation, lat_str, lon_str)
        
        # Use memoize to make sure that the same weather file isn't generated for different soil points
        if (lat_str, lon_str) not in memoize:
            memoize[(lat_str, lon_str)] = soil_path
        
        logging.debug(f"Closest grid point location: {grid_lat},{grid_lon}, elevation: {elevation}")
    
    gldas_elevation_ds.close()

    num_weather_points = len(memoize)
    logging.info(f"Done fetching nearest {num_weather_points} GLDAS grid points for {num_soil_points} Soil points")

    cur_start_date = start_date
    while cur_start_date < end_date:
        cur_end_date = cur_start_date + timedelta(days = batch_numdays)
        if cur_end_date > end_date:
            cur_end_date = end_date

        cur_end_date = cur_end_date - timedelta(minutes = 1) # So we don't get midnight file of next day
        
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
    
        logging.info(f"Converting GLDAS data to cycles weather data for {num_weather_points} points..")
        for lat, lon, soil_path, fname in coords:
            logging.debug("Processing data for {0}, {1}".format(lat, lon))
            (grid_lat, grid_lon, elevation, lat_str, lon_str) = soil_weather_grid_points[soil_path]

            # Only produce cycles weather file for this point once (here we're doing it for only 1 soil path)
            # - We will later use the same weather file for creating the zip for all the soil points
            # - We do it this way because it is computationally expensive to create the cycles weather file
            grid_soil_path = memoize[(lat_str, lon_str)]
            if grid_soil_path != soil_path:
                continue

            # Load GLDAS data for the exact gridpoint location
            logging.debug(f"Loading GLDAS data for grid point {grid_lat}, {grid_lon}")
            loc_ds = gldas_ds.sel(lat=grid_lat, lon=grid_lon, time=slice(cur_start_date, cur_end_date)).load()
            logging.debug("Loaded gldas data for location")

            logging.debug("Converting to Cycles input data")
            # Convert to Cycles Input
            loc_by_day_ds = convert_to_cycles_input(loc_ds) 
            logging.debug("Finished conversion to cycles input data")
            
            logging.debug("Converting weather input data to Pandas Dataframe...")
            loc_by_day_df = loc_by_day_ds.to_dataframe()
            loc_by_day_df.sort_values(by=['YEAR', 'DOY'])
            logging.debug("Finished converting to Dataframe") 

            # Write the cycles input weather file
            Path(output_path).mkdir(parents=True, exist_ok=True)
            common_weather_fname = f"cycles_weather_{lat_str}_{lon_str}.weather"
            common_weather_file = os.path.join(output_path, common_weather_fname)        

            logging.debug ("Writing the cycles input file..")
            # Create the output weather file if it doesn't exist
            if not os.path.exists(common_weather_file):
                outfp = open(common_weather_file, "w")
                outfp.write("LATITUDE %.2f\n" % (grid_lat))
                outfp.write("LONGITUDE %.2f\n" % (grid_lon))
                outfp.write("ALTITUDE %.2f\n" % (elevation))
                outfp.write("SCREENING_HEIGHT 2\n")
                outfp.write("%-8s%-8s%-8s%-8s%-8s%-8s%-8s%-8s%-8s\n" % (
                    'YEAR', 'DOY', 'PP', 'TX', 'TN', 'SOLAR', 'RHX', 'RHN', 'WIND'
                ))
                outfp.close()

            # Append to the weather file
            outfp = open(common_weather_file, "a")
            for index, row in loc_by_day_df.iterrows():
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
    for lat, lon, soil_path, fname in coords:
        if not os.path.exists(soil_path):
            continue

        logging.debug (f"Creating Cycles zip file for {grid_lat}, {grid_lon}")
        (grid_lat, grid_lon, elevation, lat_str, lon_str) = soil_weather_grid_points[soil_path]

        weather_fname = fname + ".weather"
        soil_fname = fname + ".soil"
        zip_fname = fname + "_soil_weather.zip"
        common_weather_fname = f"cycles_weather_{lat_str}_{lon_str}.weather"

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
