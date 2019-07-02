#!/usr/bin/python
# -*- coding: utf-8 -*-

from ccut.app.ccut_lib import CCUT, RET_VAL_OK, RET_STR_MAP
from dtran.argtype import ArgType
from dtran.ifunc import IFunc
from pydrepr import Graph

class UnitTransFunc(IFunc):
    id = "unit_trans"
    inputs = {"graph": ArgType.Graph(None), "unit_value": ArgType.String, \
              "unit_label": ArgType.String, "unit_desired": ArgType.String}
    outputs = {"graph": ArgType.Graph(None)}
    
    def __init__(self, graph: Graph, unit_value: str, unit_label: str, unit_desired: str):
        self.graph = graph
        self.unit_value = unit_value
        self.unit_label = unit_label
        self.unit_desired = unit_desired
        self.ccut = CCUT()
        
    def exec(self):
        for node in self.graph.nodes:
            src_unit = node.data[self.unit_label]
            conversion_value, err_code = self.ccut.canonical_transform(src_unit, self.unit_desired, 1)
            if RET_VAL_OK != err_code:
                print(f"Error in the unit conversion process['{src_unit}' --> '{self.unit_desired}']: [{err_code}] {RET_STR_MAP[err_code]}")
                return False # TODO: make sure that this terminates the process [or throw exception]
            new_obs_value = float(node.data[self.unit_value]) * conversion_value
            node.data[self.unit_value] = str(new_obs_value)
            node.data[self.unit_label] = self.unit_desired
        return {
            "graph": self.graph
        }