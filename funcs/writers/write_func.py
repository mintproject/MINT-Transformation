#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
from collections import defaultdict
from pathlib import Path
from typing import List, Union, Dict, Optional

import ujson as json
from drepr.models import SemanticModel, Node, LiteralNode, DataNode

from dtran.argtype import ArgType
from dtran.backend import SharedBackend
from dtran.ifunc import IFunc, IFuncType
from dtran.metadata import Metadata


class CSVWriteFunc(IFunc):
    id = "graph_write_func"
    description = """ A writer adapter.
    Generates a csv/json file.
    """
    func_type = IFuncType.WRITER
    friendly_name: str = "Graph to CSV"
    inputs = {
        "data": ArgType.DataSet,
        "output_file": ArgType.String,
    }
    outputs = {"output_file": ArgType.String}
    example = {
        "output_file": "example.csv",
    }

    def __init__(self, data: SharedBackend, output_file: Union[str, Path]):

        self.data = data
        self.output_file = Path(output_file)
        self.filter_func = IFunc.filter_func(filter)

        self.sm: SemanticModel = self.data._get_sm()

    def exec(self) -> dict:
        data_tuples, attr_names = self.tabularize_data()
        if self.output_file.endswith("csv"):
            CSVWriteFunc._dump_to_csv(data_tuples, attr_names, self.output_file)
        elif self.output_file.endswith("json"):
            CSVWriteFunc._dump_to_json(data_tuples, attr_names, self.output_file)
        return {"output_file": str(self.output_file)}

    def validate(self) -> bool:
        return True

    @staticmethod
    def _dump_to_csv(tabular_rows, attr_names, file_path):
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=attr_names)
            writer.writeheader()
            writer.writerows(tabular_rows)

    @staticmethod
    def _dump_to_json(tabular_rows, attr_names, file_path):
        with open(file_path, "w", newline="") as f:
            json.dump(tabular_rows, f)

    def tabularize_data(self) -> (list, set):
        main_class_node = self._find_main_class()

        id2attrs, attrs = self._sm_traverse(main_class_node, [])

        data_tuples = []
        for rid, attr2vals in id2attrs.items():
            rtuples = [[]]
            for attr in attrs:
                if attr in attr2vals:
                    attr_vals = [attr2vals[attr]] * len(rtuples)
                else:
                    attr_vals = [None] * len(rtuples)
                rtuples = rtuples * len(attr2vals[attr])
                rtuples = [x[0] + [x[1]] for x in zip(rtuples, attr_vals)]

            data_tuples.extend(rtuples)

        return data_tuples, attrs

    def _find_main_class(self):
        for class_ in self.sm.iter_class_nodes():
            is_main_class = True
            for _ in self.sm.iter_incoming_edges():
                is_main_class = False
                break
            if is_main_class:
                return class_

    def _sm_traverse(self, node: Node, visited: List[Node],) -> (list, set):

        id2attrs = defaultdict(lambda: defaultdict(list))
        attrs = []

        for edge in self.sm.iter_outgoing_edges(node.node_id):
            child_node = self.sm.nodes[edge.target_id]
            predicate_url = edge.label

            if isinstance(child_node, LiteralNode) or isinstance(child_node, DataNode):
                attrs.append(predicate_url)

                for record in self.data.cid(node.node_id).iter_records():
                    val = record.m(predicate_url)
                    if not isinstance(val, list):
                        val = [val]

                    id2attrs[record.id][predicate_url].extend(val)
            else:
                if child_node not in visited:

                    child2tuples, child_attrs = self._sm_traverse(
                        node, visited + [child_node]
                    )
                    for record in self.data.cid(node.node_id).iter_records():
                        for attr in child2tuples[record.m(predicate_url).id]:
                            id2attrs[record.id][
                                predicate_url + "---" + attr
                            ] = child2tuples[record.m(predicate_url).id][attr]

                            attrs.append(predicate_url + "---" + attr)

        return id2attrs, attrs

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata
