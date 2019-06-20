#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
from typing import List

from pydrepr.graph import Node, Graph

from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class WriteFunc(IFunc):
    id = "write_func"
    inputs = {"graph": ArgType.Graph(None), "main_class": ArgType.String, "output_file": ArgType.FilePath}
    outputs = {"data": ArgType.String}

    def __init__(self, graph: Graph, main_class: str, output_file: str):
        self.graph = graph
        self.main_class = main_class

        self.output_file = output_file

    def exec(self) -> dict:
        all_data_rows, attr_names = self.tabularize_data()
        if self.output_file.endswith("csv"):
            WriteFunc._dump_to_csv(all_data_rows, attr_names, self.output_file)
        elif self.output_file.endswith("json"):
            WriteFunc._dump_to_json(all_data_rows, attr_names, self.output_file)
        else:
            all_data_rows = []
        return {"data": all_data_rows}

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
        pass

    def tabularize_data(self) -> (list, set):
        main_class_nodes = []
        for node in self.graph.nodes:
            if node.data["@type"] == self.main_class:
                main_class_nodes.append(node)

        all_data_rows = []
        all_attr_names = set()
        for idx, node in enumerate(main_class_nodes):
            dict_data_rows, attr_names = self._divide_search(node, [])
            all_data_rows.extend(dict_data_rows)

            if idx == 0:
                all_attr_names = attr_names
            else:
                all_attr_names = all_attr_names.union(attr_names)

        return all_data_rows, all_attr_names

    def _divide_search(
        self, node: Node, visited: List[Node], with_ids: bool = False, excluding_attrs=None
    ) -> (list, set):
        if excluding_attrs is None:
            excluding_attrs = ["@type"]

        tuple_data_rows = [[]]
        for attr, value in node.data.items():
            tuple_data_rows[0].append((attr, value, node.id))

        for child_node in [self.graph.nodes[self.graph.edges[eid].target] for eid in node.edges_out]:
            if child_node not in visited:
                child_data_rows = self._divide_search(node, visited + [child_node])
                tuple_data_rows = tuple_data_rows * len(child_data_rows)
                tuple_data_rows = [x[0] + x[1] for x in zip(tuple_data_rows, child_data_rows)]

        dict_data_rows = []
        attr_list = set()

        for row in tuple_data_rows:
            attr_to_value_id = {}
            for attr, value, _id in row:
                if attr in excluding_attrs:
                    continue
                attr_list.add(attr)
                if with_ids:
                    attr_to_value_id[attr] = (value, _id)
                else:
                    attr_to_value_id[attr] = value
            dict_data_rows.append(attr_to_value_id)
        return dict_data_rows, attr_list
