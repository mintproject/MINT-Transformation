#!/usr/bin/python
# -*- coding: utf-8 -*-

from drepr import Graph

from dtran.argtype import ArgType, Callable, Dict, Any
from dtran.ifunc import IFunc


class LambdaTransFunc(IFunc):
    id = "lambda_trans"
    description = ''' A transformation adapter.
    Alters the graph by performing a transformation function (lambda transformation).
    '''
    inputs = {"graph": ArgType.Graph(None), "transform_func": ArgType.Function,
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
