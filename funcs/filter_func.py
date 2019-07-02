#!/usr/bin/python
# -*- coding: utf-8 -*-

import ujson
from typing import Union
from pathlib import Path
from pydrepr import Graph, Repr

from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class FilterFunc(IFunc):
    id = "filter_func"
    inputs = {"data": ArgType.Graph(None), "query": ArgType.String}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, data: Graph, query: str):
        self.data = data
        
        # rewrite the query
        conditions = []
        for and_expr in query.split(" and "):
            and_expr = and_expr.strip()
            if " = " in and_expr:
                field, value = and_expr.split(" = ")
                conditions.append(f"(n[\"{field}\"] = {value})")
            elif ".contains(" in and_expr:
                field, value = and_expr.split(".contains(")
                conditions.append(f"(n[\"{field}\"].find({value[:-1]}) != -1)")
            elif " in " in and_expr:
                field, value = and_expr.split(".contains(")
                conditions.append(f"(n[\"{field}\"] in {value})")
            else:
                raise NotImplementedError(f"Doesn't handle {and_expr} yet")

        conditions.join(" and ")
        

    def exec(self) -> dict:
        return {"data": self.data}

    def validate(self) -> bool:
        return True
