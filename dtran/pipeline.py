#!/usr/bin/python
# -*- coding: utf-8 -*-

from typing import *
from dtran.ifunc import IFunc
from dtran.wireio import WiredIOArg
from marshmallow import Schema, fields, ValidationError
from networkx import DiGraph, topological_sort, NetworkXUnfeasible


class Pipeline(object):
    def __init__(self, func_classes: List[Type[IFunc]], wired: List[any] = None):
        """
        :param func_classes:
        :param wired: input, output
        """
        # map from function id to a tuple (idx of function, order of function (start from 1)).
        self.id2order = {}
        # map from idx of function to its order
        idx2order = {}

        for i, func_cls in enumerate(func_classes):
            if func_cls.id not in self.id2order:
                self.id2order[func_cls.id] = []
            self.id2order[func_cls.id].append((i, len(self.id2order[func_cls.id]) + 1))
            idx2order[i] = len(self.id2order[func_cls.id])

        wired = wired or []
        for input, output in wired:
            if input[1] is None:
                input[1] = self.get_func_order(input[0])
            if output[1] is None:
                output[1] = self.get_func_order(output[0])

        self.wired = {}
        # applying topological sort on func_classes to determine execution order based on wiring
        graph = DiGraph()
        graph.add_nodes_from(range(len(func_classes)))
        for i, o in wired:
            input_arg = func_classes[self.id2order[i[0]][i[1] - 1][0]].inputs[i[2]]
            output_arg = func_classes[self.id2order[o[0]][o[1] - 1][0]].outputs[o[2]]
            if input_arg != output_arg:
                raise ValidationError(f"Incompatible ArgType while wiring {input_arg} to {output_arg}")
            self.wired[WiredIOArg.get_arg_name(i[0], i[1], i[2])] = WiredIOArg.get_arg_name(o[0], o[1], o[2])
            graph.add_edge(self.id2order[o[0]][o[1] - 1][0], self.id2order[i[0]][i[1] - 1][0])

        self.func_classes = []
        self.idx2order = {}
        try:
            # reordering func_classes in topologically sorted order for execution
            for i in topological_sort(graph):
                self.func_classes.append(func_classes[i])
                # changing idx of functions to map to their new order
                self.idx2order[len(self.func_classes) - 1] = idx2order[i]

        except NetworkXUnfeasible:
            raise ValidationError("Pipeline is not a DAG")

        self.schema = {}
        for i, func_cls in enumerate(self.func_classes):
            for argname in func_cls.inputs:
                gname = WiredIOArg.get_arg_name(func_cls.id, self.idx2order[i], argname)
                if gname in self.wired:
                    continue
                argtype = func_cls.inputs[argname]
                self.schema[gname] = fields.Raw(required=not argtype.optional, validate=argtype.validate)
        self.schema = Schema.from_dict(self.schema)

    def exec(self, inputs: dict) -> dict:
        inputs_copy = {}
        for arg in inputs:
            if isinstance(arg, WiredIOArg):
                if arg.func_idx is None:
                    func_idx = self.get_func_order(arg.func_id)
                else:
                    func_idx = arg.func_idx
                inputs_copy[WiredIOArg.get_arg_name(arg.func_id, func_idx, arg.name)] = inputs[arg]
            else:
                inputs_copy[arg] = inputs[arg]
        inputs = inputs_copy
        self.validate(inputs)

        output = {}
        for i, func_cls in enumerate(self.func_classes):
            func_args = {}
            for argname in func_cls.inputs.keys():
                gname = WiredIOArg.get_arg_name(func_cls.id, self.idx2order[i], argname)
                if gname in self.wired:
                    # wired has higher priority
                    func_args[argname] = output[self.wired[gname]]
                else:
                    try:
                        func_args[argname] = inputs[gname]
                    except KeyError as e:
                        if func_cls.inputs[argname].optional:
                            continue
                        raise e

            try:
                func = func_cls(**func_args)
            except TypeError:
                print(f"Cannot initialize cls: {func_cls}")
                raise

            result = func.exec()
            for argname in func_cls.outputs.keys():
                try:
                    output[WiredIOArg.get_arg_name(func_cls.id, self.idx2order[i], argname)] = result[argname]
                except TypeError:
                    print(f"Error while wiring output of {func_cls} from {argname} to {WiredIOArg.get_arg_name(func_cls.id, self.idx2order[i], argname)}")
                    raise

        return output

    def validate(self, inputs: dict) -> None:
        errors = self.schema().validate(inputs)
        if errors:
            raise ValidationError(errors)

    def get_func_order(self, func_id: str) -> int:
        if len(self.id2order[func_id]) == 0:
            raise ValueError(f"Cannot wire argument to function {func_id} because it doesn't exist")

        if len(self.id2order[func_id]) > 1:
            raise ValueError(
                f"Cannot wire argument to function {func_id} because it is ambiguous (more than one function with same id)"
            )

        return self.id2order[func_id][0][-1]
