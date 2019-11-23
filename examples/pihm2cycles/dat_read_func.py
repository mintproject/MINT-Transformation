#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime

import ujson
from typing import Union
from pathlib import Path
from drepr import Graph, DRepr
from drepr.graph import Node

from dtran.argtype import ArgType
from dtran.ifunc import IFunc
import numpy as np
import pandas as pd


class ReadDatFunc(IFunc):
    id = "read_dat_func"
    description = ''' An entry point for pipeline involving PIHM format.
    Reads an PIHM output file, a Cycles soil file and a standard variable.
    The data are represented in a Graph object.
    '''
    inputs = {"dat_file": ArgType.FilePath, "cycles_file": ArgType.FilePath,
              "standard_variable": ArgType.String}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, dat_file: Union[str, Path], cycles_file: Union[str, Path], standard_variable: str):
        self.dat_path = str(dat_file)
        self.cycles_mapping_path = str(cycles_file)
        self.standard_variable = standard_variable

    def exec(self) -> dict:
        data_array = np.fromfile(self.dat_path)
        cycle_df = pd.read_csv(self.cycles_mapping_path)

        num_ele = int(data_array[0])
        num_time = int((data_array.shape[0] - 2) // num_ele)

        nodes = []

        for idx in range(num_time):
            time1 = data_array[2 + idx * (num_ele + 1)]
            for ele_id in range(num_ele):
                node = Node(len(nodes), {}, [], [])
                node.data["schema:recordedAt"] = time1
                node.data["mint:index"] = cycle_df.iloc[ele_id, 0]
                node.data[self.standard_variable] = data_array[2 + idx * (num_ele + 1) + (ele_id + 1)]
                nodes.append(node)

        g = Graph(nodes, [])

        return {"data": g}

    @staticmethod
    def float2time(date_as_number):
        day = int(date_as_number % 100)
        date_as_number = date_as_number // 100

        month = int(date_as_number % 100)
        year = int(date_as_number // 100)

        return datetime.datetime.strptime(f"{year:d}-{month:d}-{day:d} 00:00:00", '%Y-%m-%d %H:%M:%S')

    def validate(self) -> bool:
        return True
