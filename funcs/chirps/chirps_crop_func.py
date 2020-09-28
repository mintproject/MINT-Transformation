"""
This script downloads CHIRPS global daily dataset and register with dcat
"""
import os
import urllib
import datetime
import netCDF4
import xarray

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType


class CHIRPSCropFunc(IFunc):
    id = "chirps_crop_func"
    description = """
    An adapter that downloads CHIRPS dataset, crop by given time and spatial constraint.
    """
    func_type = IFuncType.CROPPING_TRANS
    friendly_name: str = "CHIRPS crop func"
    inputs = {
        "start_date": ArgType.String,
        "end_date": ArgType.String,
        "lat_min": ArgType.Number(optional=True),
        "long_min": ArgType.Number(optional=True),
        "lat_max": ArgType.Number(optional=True),
        "long_max": ArgType.Number(optional=True),
        "output_file": ArgType.String
    }
    outputs = {}
    example = {}

    def __init__(
        self, start_date, end_date, output_file,
        lat_min=-50.0, long_min=-180.0, lat_max=50.0, long_max=180.0
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.lat_min = lat_min
        self.long_min = long_min
        self.lat_max = lat_max
        self.long_max = long_max
        self.output_file = output_file

    def exec(self) -> dict:
        # based on time constraint: download chirps files
        start_date = datetime.datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(self.end_date, '%Y-%m-%d')

        CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25"
        CHIRPS_DOWNLOAD_DIR = "/tmp/chirps"

        if not os.path.exists(CHIRPS_DOWNLOAD_DIR):
            os.makedirs(CHIRPS_DOWNLOAD_DIR)

        chirps_urls = [f"{CHIRPS_BASE_URL}/chirps-v2.0.{year}.days_p25.nc" for year in range(start_date.year, end_date.year + 1, 1)]
        fns = []

        for url in chirps_urls:
            chirps_fn = os.path.join(CHIRPS_DOWNLOAD_DIR, url.split("/")[-1])
            if not os.path.exists(chirps_fn):
                print(f"Downloading {url} to {chirps_fn}...")
                urllib.request.urlretrieve(url, chirps_fn)
            else:
                print(f"{chirps_fn} is already downloaded...")
            fns.append(chirps_fn)

        # Given the files, crop by time and spatial constraints
        # Use xarray date indexing: http://xarray.pydata.org/en/stable/time-series.html
        chirps_precip = xarray.open_mfdataset(fns, combine='by_coords')
        precip = chirps_precip.precip.sel(
            time=slice(self.start_date, self.end_date),
            latitude=slice(self.lat_min, self.lat_max),
            longitude=slice(self.long_min, self.long_max)
        )
        print(precip)

        # Write out to output file: force faster
        # See issue: https://github.com/pydata/xarray/issues/2912
        precip.load().to_netcdf(self.output_file)
        print(f" Writing the result to {self.output_file}")
        return

    def validate(self) -> bool:
        return True