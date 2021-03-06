#!/usr/bin/python
# -*- coding: utf-8 -*-

from ccut import ccut, RET_VAL_OK
from dtran.argtype import ArgType, Optional
from dtran.ifunc import IFunc, IFuncType
from drepr import Graph


class UnitTransFunc(IFunc):
    id = "unit_trans"
    description = ''' A transformation adapter.
    Alters the graph by performing unit conversion on some values in the graph.
    '''
    inputs = {
        "data": ArgType.DataSet(None),
        "unit_value": ArgType.String,
        "unit_label": ArgType.String,
        "unit_desired": ArgType.String,
        "filter": ArgType.String(True),
    }
    outputs = {"data": ArgType.DataSet(None)}

    friendly_name: str = "Unit Transformation Function"
    func_type = IFuncType.UNIT_TRANS
    example = {
        "unit_value": "dcat: measure_1_value",
        "unit_label": "sdmx-attribute:unitMeasure",
        "unit_desired": "$/kg",
        # TODO: not sure about this one
        "filter": "@type = 'qb:Observation' and sdmx-attribute:refArea.contains('Aweil (Town)')",
    }

    def __init__(
        self, graph: Graph, unit_value: str, unit_label: str, unit_desired: str, filter: Optional[str] = None
    ):
        self.graph = graph
        self.unit_value = unit_value
        self.unit_label = unit_label
        self.unit_desired = unit_desired
        self.filter_func = IFunc.filter_func(filter)
        self.ccut = ccut()

    def exec(self):
        for node in self.graph.nodes:
            if self.filter_func(node):
                src_unit = node.data[self.unit_label]
                conversion_value, err_code, _, _, _ = self.ccut.convert_str2str(src_unit, self.unit_desired, 1)
                if RET_VAL_OK != err_code:
                    continue
                new_obs_value = float(node.data[self.unit_value]) * conversion_value
                node.data[self.unit_value] = str(new_obs_value)
                node.data[self.unit_label] = self.unit_desired
        return {"data": self.graph}
