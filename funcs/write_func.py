#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
from pathlib import Path

import numpy as np
from typing import List, Union, Dict
import ujson as json
from pydrepr.graph import Node, Graph

from dtran.argtype import ArgType
from dtran.ifunc import IFunc
import netCDF4 as nc4


class WriteFuncNDimArray(IFunc):
    id = "write_func_ndarray"
    inputs = {
        "data": ArgType.NDimArray,
        "main_class": ArgType.String,
        "output_file": ArgType.FilePath
    }
    outputs = {"data": ArgType.String}

    def __init__(self, data: np.ndarray, main_class: str, output_file: str):
        self.data = data
        self.main_class = main_class
        self.output_file = output_file

    def exec(self) -> dict:
        f = nc4.Dataset(self.output_file, "w", format="NETCDF4")
        tempgrp = f.createGroup(self.main_class)
        idx = 0
        dims = []

        tempgrp.createDimension("lon", self.data.shape[0] - 1)
        tempgrp.createDimension("lat", self.data.shape[1] - 1)
        tempgrp.createDimension("z", self.data.shape[2] - 1)
        tempgrp.createDimension("time", None)

        longitude = tempgrp.createVariable("Longitude", "f4", "lon")
        latitude = tempgrp.createVariable("Latitude", "f4", "lat")
        levels = tempgrp.createVariable("Levels", "i4", "z")
        temp = tempgrp.createVariable("Value", "f4", ("time", "lon", "lat", "z"))

        longitude[:] = self.data[0, :, :]
        latitude[:] = self.data[:, 0, :]
        levels[:] = self.data[:, :, 0]
        temp[0, :, :, :] = self.data[1:, 1:, 1:]

        f.close()

    def validate(self) -> bool:
        return True


class WriteFuncGraph(IFunc):
    id = "write_func_graph"
    inputs = {
        "graph": ArgType.Graph(None),
        "main_class": ArgType.String,
        "output_file": ArgType.FilePath,
        "mapped_columns": ArgType.OrderedDict(None),
    }
    outputs = {"data": ArgType.String}

    def __init__(self, graph: Graph, main_class: str, output_file: Union[str, Path],
                 mapped_columns: Dict[str, str]):
        self.graph = graph
        self.main_class = main_class
        self.mapped_columns = mapped_columns

        self.output_file = str(output_file)

    def exec(self) -> dict:
        all_data_rows, attr_names = self.tabularize_data()
        if self.output_file.endswith("csv"):
            WriteFuncGraph._dump_to_csv(all_data_rows, attr_names, self.output_file)
        elif self.output_file.endswith("json"):
            WriteFuncGraph._dump_to_json(all_data_rows, attr_names, self.output_file)
        else:
            all_data_rows = []
        return {"data": all_data_rows}

    def validate(self) -> bool:
        return True

    @staticmethod
    def _dump_to_csv(tabular_rows, attr_names, file_path):
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=attr_names)
            writer.writeheader()
            writer.writerows(tabular_rows)

    @staticmethod
    def _dump_to_json(tabular_rows, attr_names, file_path):
        with open(file_path, "w", newline="") as f:
            json.dump(tabular_rows, f)

    def tabularize_data(self) -> (list, set):
        main_class_nodes = []
        for node in self.graph.nodes:
            if node.data["@type"] == self.main_class:
                main_class_nodes.append(node)

        # modified code to allow rename & select a subset of attributes
        if len(self.mapped_columns) == 0:
            all_data_rows = []
            all_attr_names = set()
            for idx, node in enumerate(main_class_nodes):
                dict_data_rows, attr_names = self._divide_search(node, [])
                all_data_rows.extend(dict_data_rows)

                if idx == 0:
                    all_attr_names = attr_names
                else:
                    all_attr_names = all_attr_names.union(attr_names)

            return all_data_rows, all_attr_names
        else:
            all_data_rows = []
            for node in main_class_nodes:
                dict_data_rows, attr_names = self._divide_search(node, [])
                for row in dict_data_rows:
                    all_data_rows.append(
                        {new_k: row[old_k]
                         for old_k, new_k in self.mapped_columns.items()})

            return all_data_rows, list(self.mapped_columns.values())

    def _divide_search(self,
                       node: Node,
                       visited: List[Node],
                       with_ids: bool = False,
                       excluding_attrs=None) -> (list, set):
        if excluding_attrs is None:
            excluding_attrs = ["@type"]

        tuple_data_rows = [[]]
        for attr, value in node.data.items():
            tuple_data_rows[0].append((attr, value, node.id))

        for child_node in [
                self.graph.nodes[self.graph.edges[eid].target] for eid in node.edges_out
        ]:
            if child_node not in visited:
                child_data_rows = self._divide_search(node, visited + [child_node])
                tuple_data_rows = tuple_data_rows * len(child_data_rows)
                tuple_data_rows = [x[0] + x[1] for x in zip(tuple_data_rows, child_data_rows)]

        dict_data_rows = []
        attr_list = set()

        for row in tuple_data_rows:
            attr_to_value_id = {}
            for attr, value, _id in row:
                if attr in excluding_attrs:
                    continue
                attr_list.add(attr)
                if with_ids:
                    attr_to_value_id[attr] = (value, _id)
                else:
                    attr_to_value_id[attr] = value
            dict_data_rows.append(attr_to_value_id)
        return dict_data_rows, attr_list
