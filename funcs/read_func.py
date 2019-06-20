#!/usr/bin/python
# -*- coding: utf-8 -*-

import ujson
from pydrepr import Graph, Repr

from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class ReadFunc(IFunc):
    id = "read_func"
    inputs = {"repr_file": ArgType.FilePath, "resources": ArgType.String}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, repr_file: str, resources: str):
        if resources.startswith("{"):
            self.resources = ujson.loads(resources)
        else:
            self.resources = {"default": resources}

        self.repr = Repr.from_file(repr_file)

    def exec(self) -> dict:
        g = Graph.from_repr(self.repr, self.resources)
        return {"data": g}

    def validate(self) -> bool:
        return True
