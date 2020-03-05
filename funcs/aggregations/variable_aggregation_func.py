#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import logging
import time
import requests
from datetime import datetime
from dateutil import parser
from typing import Union, List, Dict, Iterable
from pathlib import Path
from drepr import DRepr, outputs
from drepr.models import SemanticModel
from drepr.outputs import ArrayBackend, GraphBackend
from drepr.outputs.base_lst_output_class import BaseLstOutputClass
from drepr.outputs.base_output_class import BaseOutputClass
from drepr.outputs.base_output_sm import BaseOutputSM
import subprocess

from drepr.outputs.base_record import BaseRecord
from drepr.outputs.record_id import RecordID

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from funcs.readers.dcat_read_func import ShardedBackend


class VariableAggregationFunc(IFunc):
    id = "aggregation_func"
    description = ''''''
    func_type = IFuncType.OTHERS
    friendly_name: str = "Aggregation"
    inputs = {
        "dataset": ArgType.DataSet(None),
        "group_by": ArgType.String,
        "operator": ArgType.String
    }
    outputs = {"data": ArgType.DataSet(None)}
    example = {}
    logger = logging.getLogger(__name__)

    def __init__(self, dataset, group_by: list, operator: str):
        self.dataset = dataset
        self.group_by = group_by
        self.operator = operator

    def exec(self) -> dict:
        output = {}
        rdf = self.dataset.ns(outputs.Namespace.RDF)
        mint_geo = self.dataset.ns("https://mint.isi.edu/geo")
        mint = self.dataset.ns("https://mint.isi.edu/")

        groups = {}

        if isinstance(self.dataset, ShardedBackend):
            # check if the data is partition
            for dataset in self.dataset.drain():
                pass
        else:
            for c in self.dataset.c(mint.Variable):
                for r in c.iter_records():
                    timestamp = r.s(mint.timestamp)
                    # convert timestamp into datetime and get the key
                    time_key = ()
                    location_key = ()

                    key = (time_key, location_key)
                    groups[key].append(r.s(rdf.value))

        if self.operator == "sum":
            data = [{}]

        return data

    def validate(self) -> bool:
        return True