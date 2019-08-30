#!/usr/bin/python
# -*- coding: utf-8 -*-

from ccut.app.ccut_lib import CCUT, RET_VAL_OK, RET_STR_MAP
from dtran.argtype import ArgType, Callable, Dict, Any
from dtran.ifunc import IFunc
from pydrepr import Graph


class LambdaTransFunc(IFunc):
    id = "lambda_trans"
    inputs = {"graph": ArgType.Graph(None), "transform_func": ArgType.Function, "input_label": ArgType.String,
              "output_label": ArgType.String}
    outputs = {"graph": ArgType.Graph(None)}

    def __init__(self, graph: Graph, transform_func: Callable[Dict, Any], input_label: str, output_label: str):
        self.graph = graph
        self.transform_func = transform_func
        self.input_label = input_label
        self.output_label = output_label

    def exec(self):
        for node in self.graph.nodes:
            node.data[self.output_label] = self.transform_func(node.data)
        return {
            "graph": self.graph
        }
