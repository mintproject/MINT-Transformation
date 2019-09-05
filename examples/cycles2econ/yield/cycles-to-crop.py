#!/usr/bin/env python3

"""
@author: deborahkhider
@author: rafaelfsilva

Transformation code from CYCLES output to Crop model
"""

import configparser
import csv
import os
import sys
import numpy as np
import pandas as pd
from itertools import islice

# configuration parameters
year = int(sys.argv[1])  # in its present configuration only one year is allowed

data = {}

for f in sys.argv[2:-1]:
    with open(f) as configfile:
        config_string = "".join([next(configfile) for x in range(3)])
        config = configparser.ConfigParser()
        config.read_string(config_string)

        # read properties
        crop_name = config.get("DEFAULT", "crop-name")
        percent_increase_fertilizer = float(config.get("DEFAULT", "percent-increase-fertilizer")) / 100

        # basic properties
        if crop_name not in data:
            data[crop_name] = {}
        if percent_increase_fertilizer != 0.0:
            data[crop_name]["percent_increase_fertilizer"] = percent_increase_fertilizer

    # Open the season config file and look for the grain yield
    d = pd.read_table(f, delimiter="\t", skiprows=[0, 1, 2, 3, 5], index_col=False)

    # Make sure the dates are in datetime format
    for item in list(d):
        if "DATE" in item:
            header_time = item
        if "GRAIN YIELD" in item:
            header = item

    d[header_time] = pd.to_datetime(d[header_time])
    time = d[header_time].dt.year.unique()
    grain = float(d.groupby(d[header_time].dt.year)[header].transform("mean").unique())
    data[crop_name]["base_grain" if percent_increase_fertilizer == 0.0 else "scenario_grain"] = grain

# Make sure the year is available
if year in time:
    index = np.where(time == year)[0][0]
else:
    sys.exit("Year not available in Cycles run")

# Calculate the yield %
for crop_name in data:
    data[crop_name]["percent_yield"] = (
        data[crop_name]["scenario_grain"] - data[crop_name]["base_grain"]
    ) / data[crop_name]["base_grain"]
    # elasticity
    data[crop_name]["elasticity"] = (
        data[crop_name]["percent_yield"] / data[crop_name]["percent_increase_fertilizer"]
    )

# write out as csv file
csv_file = sys.argv[-1:][0]

crops_econ = ["cassava", "groundnuts", "corn", "sesame", "sorghum"]
default_value = ["0.25", "0.25", "0.11", "0.25", "0.11"]

with open(csv_file, "w", newline="") as csvfile:
    yieldwriter = csv.writer(csvfile, delimiter=",")
    yieldwriter.writerow(["", "ybarN"])
    for idx, crop_econ in enumerate(crops_econ):
        if crop_econ in (name.lower() for name in data):
            tt = crop_econ
            if crop_econ == "corn":
                tt = "maize"
            yieldwriter.writerow([tt, "%.2f" % data[crop_econ.capitalize()]["elasticity"]])
        else:
            yieldwriter.writerow([crop_econ, default_value[idx]])
