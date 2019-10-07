#!/usr/bin/python
# -*- coding: utf-8 -*-

from typing import Dict

import ujson
from drepr import Graph

from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class GraphStr2StrFunc(IFunc):
    id = "graph_str2str_func"
    description = ''' A transformation adapter.
    Maps an existing set of strings (semantic attributes) to a new desired set of strings.
    '''
    inputs = {"graph": ArgType.Graph(None), "semantic_type": ArgType.String, "str2str": ArgType.String}
    outputs = {}

    def __init__(self, graph: Graph, semantic_type: str, str2str: Dict[str, str]):
        self.graph = graph
        self.semantic_type = semantic_type
        self.class_name, self.predicate = semantic_type.split("--")
        self.str2str = ujson.loads(str2str)

    def exec(self) -> dict:
        for node in self.graph.nodes:
            if node.data["@type"] == self.class_name and self.predicate in node.data:
                node.data[self.predicate] = self.str2str[node.data[self.predicate]]

        return {}

    def validate(self) -> bool:
        return True
