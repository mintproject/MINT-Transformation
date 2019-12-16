import xarray
from drepr import Graph
from drepr.graph import Node

from dtran import IFunc, ArgType

import argparse
import math
import os
import shutil
from datetime import datetime, timedelta

import numpy as np
from netCDF4 import Dataset


class FLDAS2CyclesFunc(IFunc):
    id = "fldas2cycles_func"

    inputs = {
        "start_date": ArgType.DateTime,
        "end_date": ArgType.DateTime,
        "latitude": ArgType.Number,
        "longitude": ArgType.Number,
        "coord_graph": ArgType.Graph,
        "gldas_ndarray": ArgType.NDimArray,
    }

    outputs = {
        "cycles_graph": ArgType.Graph
    }

    def __init__(self, start_date, end_date, latitude, longitude, coord_graph, gldas_array: xarray):
        self.start_date = start_date
        self.end_date = end_date

        self.latitude = latitude
        self.longitude = longitude
        self.coord_graph = coord_graph
        self.gldas_array = gldas_array

    @staticmethod
    def read_variables(x, y, time, nd_array):
        var_to_value = {}
        for var in ["precipitation", "temperature", "wind", "solar", "pressure", "air"]:
            var_to_value[var] = nd_array.isel(time=time, long=x, lat=y)[var]

        es = 611.2 * math.exp(
            17.67 * (var_to_value["temperature"] - 273.15) / (var_to_value["temperature"] - 273.15 + 243.5)
        )
        ws = 0.622 * es / (var_to_value["precipitation"] - es)
        w = var_to_value["air"] / (1.0 - var_to_value["air"])
        rh = min(w / ws, 1.0)
        var_to_value["rh"] = rh

        return var_to_value

    def exec(self) -> dict:
        nodes = []
        precipitation = 0.0
        tx = -999.0
        tx = -999.0
        tn = 999.0
        wind = 0.0
        solar = 0.0
        rhx = -999.0
        rhn = 999.0
        counter = 0

        for t in self.gldas_array.coords["date"].values:
            for x in self.gldas_array.coords["long"].values:
                for y in self.gldas_array.coords["lat"].values:
                    var_to_value = FLDAS2CyclesFunc.read_variables(t, x, y)

                    precipitation += var_to_value["precipitation"]

                    tx = max(tx, var_to_value["temperature"])
                    tn = min(tn, var_to_value["temperature"])

                    wind += var_to_value["wind"]

                    solar += var_to_value["solar"]

                    rhx = max(rhx, var_to_value["rh"])
                    rhn = min(rhn, var_to_value["rh"])

                    counter += 1
                    precipitation /= float(counter)
                    precipitation *= 86400.0

                    wind /= float(counter)

                    solar /= float(counter)
                    solar *= 86400.0 / 1.0e6

                    rhx *= 100.0
                    rhn *= 100.0

                    tx -= 273.15
                    tn -= 273.15

                    node = Node(len(nodes), {}, [], [])
                    node.data["mint:precipitation"] = precipitation
                    node.data["mint:min_temperature"] = tn
                    node.data["mint:max_temperature"] = tx
                    node.data["mint:solar"] = solar
                    node.data["mint:min_rh"] = rhn
                    node.data["mint:max_rh"] = rhx
                    node.data["mint:longitude"] = x
                    node.data["mint:latitude"] = y

                    nodes.append(node)
        return {"cycles_graph": Graph(nodes)}
