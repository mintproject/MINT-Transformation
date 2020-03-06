#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
from collections import defaultdict
from pathlib import Path
from typing import List, Union, Dict, Optional

import ujson as json
from drepr.models import SemanticModel, Node, LiteralNode, DataNode
from drepr.outputs.base_output_sm import BaseOutputSM

from dtran.argtype import ArgType
from dtran.backend import ShardedBackend
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
        "data": ArgType.DataSet(None),
        "output_file": ArgType.String,
    }
    outputs = {"output_file": ArgType.String}
    example = {
        "output_file": "example.csv",
    }

    def __init__(
        self, data: Union[BaseOutputSM, ShardedBackend], output_file: Union[str, Path]
    ):

        self.data = data
        self.output_file = Path(output_file)

        self.sm: SemanticModel = self.data.get_sm()

    def exec(self) -> dict:
        data_tuples, attr_names = self.tabularize_data()
        print("Finish tabularizing")
        if self.output_file.suffix == ".csv":
            CSVWriteFunc._dump_to_csv(data_tuples, attr_names, self.output_file)
        elif self.output_file.suffix == ".json":
            CSVWriteFunc._dump_to_json(data_tuples, attr_names, self.output_file)
        return {"output_file": str(self.output_file)}

    def validate(self) -> bool:
        return True

    @staticmethod
    def _dump_to_csv(tabular_rows, attr_names, file_path):
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(attr_names)
            writer.writerows(tabular_rows)

    @staticmethod
    def _dump_to_json(tabular_rows, attr_names, file_path):
        with open(file_path, "w", newline="") as f:
            json.dump(tabular_rows, f)

    def tabularize_data(self) -> (list, set):
        main_class_node = self._find_main_class()

        data_tuples = []
        main_attrs = []

        if isinstance(self.data, ShardedBackend):

            for dataset in self.data.datasets:
                id2attrs, attrs = self._sm_traverse(dataset, main_class_node, [])

                main_attrs = attrs

                for rid, attr2vals in id2attrs.items():
                    rtuples = [[]]
                    for attr in attrs:
                        if attr in attr2vals:
                            attr_vals = [attr2vals[attr]] * len(rtuples)
                        else:
                            attr_vals = [None] * len(rtuples)
                        rtuples = rtuples * len(attr2vals[attr])
                        rtuples = [x[0] + x[1] for x in zip(rtuples, attr_vals)]

                    data_tuples.extend(rtuples)
        else:
            id2attrs, attrs = self._sm_traverse(self.data, main_class_node, [])

            main_attrs = attrs

            for rid, attr2vals in id2attrs.items():
                rtuples = [[]]
                for attr in attrs:
                    if attr in attr2vals:
                        attr_vals = [attr2vals[attr]] * len(rtuples)
                    else:
                        attr_vals = [None] * len(rtuples)
                    rtuples = rtuples * len(attr2vals[attr])
                    rtuples = [x[0] + x[1] for x in zip(rtuples, attr_vals)]

                data_tuples.extend(rtuples)

        return data_tuples, main_attrs

    def _find_main_class(self):
        for class_ in self.sm.iter_class_nodes():
            is_main_class = True
            for _ in self.sm.iter_incoming_edges(class_.node_id):
                is_main_class = False
                break
            if is_main_class:
                return class_

    def _sm_traverse(
        self, dataset: BaseOutputSM, node: Node, visited: List[Node],
    ) -> (list, set):
        print(f"Called {node.node_id}")
        count = 0
        id2attrs = defaultdict(lambda: defaultdict(list))
        attrs = set()

        for record in dataset.cid(node.node_id).iter_records():
            count += 1
        print("Count: ", count)

        for edge in self.sm.iter_outgoing_edges(node.node_id):
            child_node = self.sm.nodes[edge.target_id]
            predicate_url = edge.label
            print(node.node_id, child_node.node_id)

            if isinstance(child_node, LiteralNode) or isinstance(child_node, DataNode):
                attrs.add(predicate_url)

                for record in dataset.cid(node.node_id).iter_records():
                    val = record.m(predicate_url)
                    if not isinstance(val, list):
                        val = [val]
                    id2attrs[record.id][predicate_url].extend(val)
            else:
                if child_node not in visited:

                    child2tuples, child_attrs = self._sm_traverse(
                        dataset, child_node, visited + [child_node]
                    )
                    for record in dataset.cid(node.node_id).iter_records():
                        rids = record.m(predicate_url)
                        for rid in rids:
                            child_record = dataset.get_record_by_id(rid)
                            for attr in child2tuples[child_record.id]:
                                id2attrs[record.id][
                                    predicate_url + "---" + attr
                                ] = child2tuples[child_record.id][attr]

                                attrs.add(predicate_url + "---" + attr)

        return id2attrs, list(attrs)

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata
