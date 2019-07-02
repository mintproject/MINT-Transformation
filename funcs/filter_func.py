#!/usr/bin/python
# -*- coding: utf-8 -*-

from pydrepr import Graph

from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class FilterFunc(IFunc):
    id = "filter_func"
    inputs = {"data": ArgType.Graph(None), "filter": ArgType.String}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, data: Graph, filter: str):
        self.data = data

        # rewrite the query
        conditions = []
        for and_expr in filter.split(" and "):
            and_expr = and_expr.strip()
            if " = " in and_expr:
                field, value = and_expr.split(" = ")
                conditions.append(f"(n.data[\"{field}\"] == {value})")
            elif ".contains(" in and_expr:
                field, value = and_expr.split(".contains(")
                conditions.append(f"(n.data[\"{field}\"].find({value[:-1]}) != -1)")
            elif " in " in and_expr:
                field, value = and_expr.split(" in ")
                conditions.append(f"(n.data[\"{field}\"] in {value})")
            else:
                raise NotImplementedError(f"Doesn't handle {and_expr} yet")

        self.func = eval(f"lambda n: " + " and ".join(conditions))

    def exec(self) -> dict:
        nodes = []
        for node in self.data.nodes:
            if self.func(node):
                nodes.append(node)
                # TODO: fix me
                assert len(node.edges_in) == 0 and len(node.edges_out) == 0

        g = Graph(self.data.prefixes, nodes, [])
        return {"data": g}

    def validate(self) -> bool:
        return True
