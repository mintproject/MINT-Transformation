#!/usr/bin/python
# -*- coding: utf-8 -*-
import bisect
import math
import subprocess, numpy
from pathlib import Path
from typing import List, Dict, Tuple, Union



def read_points(cell_file: str) -> List[dict]:
    conf = get_lib_config(__file__)

    # invoke the R script to transform cell_file into points.csv
    subprocess.check_output(
        conf["cells2points_cmd"].replace("${cell_file}", cell_file),
        shell=True)

    # the file is generated and put in "/tmp/dtran_pihm2netcdf_surf_points.csv"
    pts_repr = Representation.deserialize(
        deserialize_yml(Path(__file__).parent / "resources/points.repr.yml"))
    pts_ds = DataSource(
        pts_repr, CSVRAReader(Path("/tmp/dtran_pihm2netcdf_surf_points.csv")))

    points = pts_ds.select(
        query2sg(
            pts_repr.semantic_model.ont, {
                "main_node":
                "pihm:Point1",
                "edges": [
                    "pihm:Point1--mint:index",
                    "pihm:Point1--schema:latitude",
                    "pihm:Point1--schema:longitude",
                ]
            })).exec()
    for point in points:
        point["https://mint.isi.edu/index"] += 1
    return points


def points2matrix(cell_file: str, mean_space: Union[str, float] = "auto"
                  ) -> Tuple[List[List[float]], Dict[int, Tuple[int, int]],
                             List[float], List[float]]:
    points = read_points(cell_file)
    mint_index = "https://mint.isi.edu/index"
    schema_lat = "https://schema.org/latitude"
    schema_long = "https://schema.org/longitude"

    ylat = sorted({n[schema_lat] for n in points})
    xlong = sorted({n[schema_long] for n in points})

    if mean_space == "auto":
        mean_space_long = numpy.mean(
            [i - j for i, j in zip(xlong[1:], xlong[:-1])])
        mean_space_lat = numpy.mean(
            [i - j for i, j in zip(ylat[1:], ylat[:-1])])
    else:
        mean_space_lat, mean_space_long = mean_space, mean_space

    xlong = get_evenly_spacing_axis(
        min(xlong), max(xlong), mean_space_long, True)
    ylat = get_evenly_spacing_axis(min(ylat), max(ylat), mean_space_lat, True)

    point2idx = {}
    matrix = [[-999.0 for __ in range(len(ylat))] for _ in range(len(xlong))]

    for point in points:
        xi = bisect.bisect(xlong, point[schema_long]) - 1
        yi = bisect.bisect(ylat, point[schema_lat]) - 1
        point2idx[point[mint_index]] = (xi, yi)

    return matrix, point2idx, xlong, ylat


def get_evenly_spacing_axis(vmin: float, vmax: float, spacing: float,
                            is_rouding_point: bool) -> List[float]:
    if is_rouding_point:
        vmin = vmin - vmin % spacing

    n_values = math.ceil((vmax - vmin) / spacing) + 1
    axis = [vmin + spacing * i for i in range(n_values + 1)]
    if axis[-2] > vmax:
        axis.pop()
    return axis
