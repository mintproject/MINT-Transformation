#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
from collections import defaultdict

import numpy as np
import xarray as xr
from netCDF4 import Dataset
from drepr import Graph
from scipy import stats

from dtran import ArgType
from examples.pihm2netcdf.pihm_flooding_index_func import PihmFloodingIndexFunc


class PihmMonthlyAverageFloodingFunc(PihmFloodingIndexFunc):
    id = "pihm_monthly_average_flooding_func"
    inputs = {
        "graph": ArgType.Graph(None),
        "mean_space": ArgType.String,
        "start_time": ArgType.DateTime,
        "end_time": ArgType.DateTime,
        "threshold": ArgType.Number,
    }
    outputs = {"data": ArgType.NDimArray}

    def __init__(
            self,
            graph: Graph,
            mean_space: str,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            threshold: float,
    ):
        super().__init__(graph, mean_space, start_time, threshold)
        self.graph = graph

        if mean_space != "auto":
            mean_space = float(mean_space)
        self.mean_space = mean_space
        self.start_time = start_time
        self.end_time = end_time
        self.threshold = threshold

    def exec(self) -> dict:
        matrix, point2idx, xlong, ylat = self._points2matrix(self.mean_space)
        max_flooding = 0

        indices = defaultdict(lambda: [])
        flood_ndarray = np.ones((12, len(xlong), len(ylat)), dtype=np.float32) * -999.0

        for node in self.graph.iter_nodes():
            xi, yi = point2idx[node.data["mint:index"]]
            recorded_at = (self.start_time + datetime.timedelta(minutes=node.data["schema:recordedAt"] - 1440)).month - 1

            flooding_value = node.data["mint:flooding"]
            max_flooding = max(max_flooding, flooding_value)

            indices[(recorded_at, xi, yi)].append(flooding_value)

        for (i, j, k), val in indices.items():
            flood_ndarray[i][j][k] = np.mean(val)

        time = [datetime.date(self.start_time.year, month, self.start_time.day).strftime('%Y-%m-%dT%H:%M:%SZ') for
                      month in range(1, 13)]

        flood_var = xr.DataArray(
            flood_ndarray,
            coords=[("time", time), ("X", xlong), ("Y", ylat)],
            attrs={
                "title": "Surface Inundation",
                "standard_name": "land_water_surface__height_flood_index",
                "long_name": "Surface Inundation",
                "units": "m",
                "valid_min": 0.0,
                "valid_max": np.max(flood_ndarray),
                "missing_value": -999.0,
                "fill_value": -999.0,
            },
        )
        # time_var = xr.DataArray(time, coords=[("time", time)])
        # xlong_var = xr.DataArray(xlong, coords=[("X", xlong)], attrs={
        #     "standard_name": "longitude",
        #     "long_name": "longitude",
        #     "axis": "X",
        #     "units": "degrees_east",
        # })
        # ylat_var = xr.DataArray(ylat, coords=[("Y", ylat)], attrs={
        #     "standard_name": "latitude",
        #     "long_name": "latitude",
        #     "axis": "Y",
        #     "units": "degrees_north",
        # })

        time_resolution = "P1M"

        flood_dataset = xr.Dataset(
            data_vars={"flood": flood_var},
            attrs={
                "title": "Monthly gridded surface inundation for Pongo River in 2017",
                "comment": "Outputs generated from the workflow",
                "naming_authority": "edu.isi.workflow",
                "id": "/MINT/NETCDF/MONTHLY_GRIDDED_SURFACE_INUNDATION_2017",
                "date_created": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "date_modified": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "creator_name": "Minh Pham",
                "creator_email": "minhpham@usc.edu",
                "time_coverage_start": self.start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "time_coverage_end": self.end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "time_coverage_resolution": time_resolution,
            },
        )

        x_attrs = {
            "standard_name": "longitude",
            "long_name": "longitude",
            "axis": "X",
            "units": "degrees_east",
        }
        y_attrs = {
            "standard_name": "latitude",
            "long_name": "latitude",
            "axis": "Y",
            "units": "degrees_north",
        }

        flood_dataset.X.attrs.update(x_attrs)
        flood_dataset.Y.attrs.update(y_attrs)

        return {"data": flood_dataset}

    def validate(self) -> bool:
        return True
