#!/usr/bin/python
# -*- coding: utf-8 -*-
import glob
from pathlib import Path
from typing import Union

import ujson
from drepr import Graph, DRepr
from drepr.outputs import ArrayBackend, GraphBackend

from dtran.argtype import ArgType
from dtran.backend import ShardedBackend
from dtran.ifunc import IFunc, IFuncType


class ReadFunc(IFunc):
    id = "read_func"
    description = """ An entry point in the pipeline.
    Reads an input file (or multiple files) and a yml file describing the D-REPR layout of each file.
    Return a Dataset object 
    """
    friendly_name: str = "Local File Reader"
    func_type = IFuncType.READER
    inputs = {"repr_file": ArgType.FilePath, "resource_path": ArgType.FilePath}
    outputs = {"data": ArgType.DataSet(None)}
    example = {
        "repr_file": "./wfp_food_prices_south-sudan.repr.yml",
        "resources": "./wfp_food_prices_south-sudan.csv",
    }

    def __init__(self, repr_file: Union[str, Path], resource_path: Union[str, Path]):
        resource_path = str(resource_path)

        self.repr = DRepr.parse_from_file(str(repr_file))
        self.resources = glob.glob(resource_path)

        assert len(self.resources) > 0

    def exec(self) -> dict:
        if self.get_preference("data") is None or self.get_preference("data") == "array":
            backend = ArrayBackend
        else:
            backend = GraphBackend

        if len(self.resources) == 1:
            return {
                "data": backend.from_drepr(self.repr, self.resources[0])
            }
        else:
            dataset = ShardedBackend(len(self.resources))
            for resource in self.resources:
                dataset.add(
                    backend.from_drepr(self.repr, resource, dataset.inject_class_id)
                )
            return {"data": dataset}

    def validate(self) -> bool:
        return True
