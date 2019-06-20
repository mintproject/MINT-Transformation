#!/usr/bin/python
# -*- coding: utf-8 -*-

from typing import *
from dtran.ifunc import IFunc
from dtran.wireio import WiredIOArg


class Pipeline(object):
    def __init__(self, func_cls: List[Type[IFunc]], wired: List[Tuple]):
        """
        :param func_cls:
        :param wired: input, output
        """
        self.func_cls = func_cls
        for input, output in wired:
            if input[1] is None:
                input[1] = self.get_func_idx(input[0])
            if output[1] is None:
                output[1] = self.get_func_idx(output[0])

        self.wired = {
            WiredIOArg.get_arg_name(i[0], i[1], i[2]): WiredIOArg.get_arg_name(o[0], o[1], o[2])
            for i, o in wired
        }

    def exec(self, inputs: dict) -> dict:
        args = list(inputs.keys())
        for arg in args:
            if isinstance(arg, WiredIOArg):
                if arg.func_idx is None:
                    func_idx = self.get_func_idx(arg.func_id)
                else:
                    func_idx = arg.func_idx
                inputs[WiredIOArg.get_arg_name(arg.func_id, func_idx, arg.name)] = inputs.pop(arg)

        output = {}
        for i, func_cls in enumerate(self.func_cls):
            func_args = {}
            for argname in func_cls.inputs.keys():
                gname = WiredIOArg.get_arg_name(func_cls.id, i, argname)
                if gname in self.wired:
                    # wired has higher priority
                    func_args[argname] = output[self.wired[gname]]
                else:
                    func_args[argname] = inputs[gname]

            func = func_cls(**func_args)
            result = func.exec()
            for argname in func_cls.outputs.keys():
                output[f"{func_cls.id}__{i}__{argname}"] = result[argname]

        return output

    def validate(self) -> bool:
        """
        Validate if the pipeline is correct
        :param self:
        :return:
        """
        return True

    def get_func_idx(self, func_id: str) -> int:
        idx = []
        for i, func_cls in enumerate(self.func_cls):
            if func_cls.id == func_id:
                idx.append(i)
        if len(idx) == 0:
            raise ValueError(f"Cannot wire argument to function {func_id} because it doesn't exist")

        if len(idx) > 1:
            raise ValueError(f"Cannot wire argument to function {func_id} because it is ambiguous (more than one function with same id)")

        return idx[0]

