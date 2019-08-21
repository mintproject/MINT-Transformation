#!/usr/bin/python
# -*- coding: utf-8 -*-

import ujson
from typing import Union
from pathlib import Path
from pydrepr import Graph, Repr

from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class ReadFunc(IFunc):
    id = "read_func"
    description = ''' An entry point in the pipeline.
    Reads an input file and a yml file describing the D-REPR layout of this file.
    The data are representated in a Graph object.
    '''
    inputs = {"repr_file": ArgType.FilePath, "resources": ArgType.String}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, repr_file: Union[str, Path], resources: Union[str, Path]):
        resources = str(resources)

        if resources.startswith("{"):
            self.resources = ujson.loads(resources)
        else:
            self.resources = {"default": resources}

        self.repr = Repr.from_file(str(repr_file))
        self.repr.validate()

    def exec(self) -> dict:
        g = Graph.from_repr(self.repr, self.resources)
        return {"data": g}

    def validate(self) -> bool:
        return True
