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


class Gldas2CyclesFunc(IFunc):
    id = "topoflow4_climate_write_func"
    description = """ A reader-transformation-writer multi-adapter.
    Creates an RTS (and RTI) file from NetCDF (climate) files.
    """
    inputs = {
        "start_date": ArgType.String,
        "end_date": ArgType.String,
        "gldas_path": ArgType.FilePath,
        "output_path": ArgType.FilePath,
        "output_prefix": ArgType.String,
        "latitude": ArgType.Number(optional=True),
        "longitude": ArgType.Number(optional=True),
        "coord_file": ArgType.FilePath(optional=True),
    }
    outputs = {"output_files": ArgType.FilePath}
    friendly_name: str = "Gldas2Cycles"
    func_type = IFuncType.MODEL_TRANS
    example = {
        "start_date": "2000-01-01",
        "end_date": "2018-01-31",
        "gldas_path": "/tmp/input/gldas",
        "output_path": "/tmp/output",
        "output_prefix": "output_prefix",
        "latitude": 30.3,
        "longitude": 125.2,
        "coord_file": "/tmp/input/oromia.csv",
    }

    def __init__(
        self,
        start_date,
        end_date,
        gldas_path,
        output_path,
        output_prefix,
        latitude=None,
        longitude=None,
        coord_file=None,
    ):
        self.coord_file = coord_file
        self.longitude = longitude
        self.latitude = latitude
        self.output_path = output_path
        self.output_prefix = output_prefix
        self.gldas_path = gldas_path
        self.end_date = end_date
        self.start_date = start_date

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        output_file = gldas2cycles(
            self.start_date,
            self.end_date,
            self.gldas_path,
            self.output_path,
            self.output_prefix,
            self.latitude,
            self.longitude,
            self.coord_file,
        )
        return {"output_files": output_file}

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata


def closest(lat, lon, path):

    elevation_fp = path + "/GLDASp4_elevation_025d.nc4"
    nc = xr.ope(elevation_fp, "r")

    best_y = (np.abs(nc.variables["lat"][:] - lat)).argmin()
    best_x = (np.abs(nc.variables["lon"][:] - lon)).argmin()

    return (
        best_y,
        best_x,
        nc["lat"][best_y],
        nc["lon"][best_x],
        nc["GLDAS_elevation"][0, best_y, best_x],
    )


def read_var(y, x, nc_name):
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

    print(datetime.strftime(t, "%Y-%m-%d"))

    nc_path = "%s/%4.4d/%3.3d/" % (path, t.timetuple().tm_year, t.timetuple().tm_yday)

    for nc_name in os.listdir(nc_path):
        if nc_name.endswith(".nc4"):
            (_prcp, _temp, _wind, _solar, _rh) = read_var(
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
    start_date,
    end_date,
    gldas_path,
    output_path,
    output_prefix,
    latitude=None,
    longitude=None,
    coord_file=None,
):

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    data_path = gldas_path

    if latitude != -1 and longitude != -1:
        coords = [(latitude, longitude, f"{output_prefix}.weather")]
    elif coord_file:
        coords = []
        with open(coord_file) as fp:
            for cnt, line in enumerate(fp):
                li = line.strip()
                if not (li.startswith("#") or li.startswith("L")):
                    nums = line.split(",")
                    lat = float(nums[0])
                    lon = float(nums[1])
                    coords.append((lat, lon, "%s-%d.weather" % (output_prefix, cnt)))
    else:
        raise ValueError("Invalid coordinates")

    memoize = {}
    fnames = []
    for lat, lon, fname in coords:
        print("Processing data for {0}, {1}".format(lat, lon))

        (y, x, grid_lat, grid_lon, elevation) = closest(lat, lon, data_path)

        if grid_lat < 0.0:
            lat_str = "%.2fS" % (abs(grid_lat))
        else:
            lat_str = "%.2fN" % (abs(grid_lat))

        if grid_lon < 0.0:
            lon_str = "%.2fW" % (abs(grid_lon))
        else:
            lon_str = "%.2fE" % (abs(grid_lon))

        if (lat_str, lon_str) in memoize:
            shutil.copyfile(memoize[(lat_str, lon_str)], fname)
            continue
        else:
            memoize[(lat_str, lon_str)] = fname

        Path(output_path).mkdir(parents=True, exist_ok=True)
        # fname = "met" + lat_str + "x" + lon_str + ".weather"
        outfp = open(os.path.join(output_path, fname), "w")
        outfp.write("LATITUDE %.2f\n" % grid_lat)
        outfp.write("ALTITUDE %.2f\n" % elevation)
        outfp.write("SCREENING_HEIGHT 2\n")
        outfp.write(
            "YEAR    DOY     PP      TX      TN     SOLAR      RHX      RHN     WIND\n"
        )

        cday = start_date

        while cday <= end_date:
            outfp.write(process_day(cday, y, x, data_path))
            cday += timedelta(days=1)

        outfp.close()
        fnames.append(fname)
    return fnames
