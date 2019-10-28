#!/usr/bin/python
# -*- coding: utf-8 -*-
from pathlib import Path
from typing import *
from dtran.ifunc import IFunc
from dtran.wireio import WiredIOArg


class Pipeline(object):
    def __init__(self, func_classes: List[Type[IFunc]], wired: List[any] = None):
        """
        :param func_classes:
        :param wired: input, output
        """
        self.func_classes = func_classes
        # map from function id to a tuple (idx of function, order of function (start from 1)).
        self.id2order = {}
        # map from idx of function to its order
        self.idx2order = {}

        for i, func_cls in enumerate(func_classes):
            if func_cls.id not in self.id2order:
                self.id2order[func_cls.id] = []
            self.id2order[func_cls.id].append((i, len(self.id2order[func_cls.id]) + 1))
            self.idx2order[i] = len(self.id2order[func_cls.id])

        wired = wired or []
        for input, output in wired:
            if input[1] is None:
                input[1] = self.get_func_order(input[0])
            if output[1] is None:
                output[1] = self.get_func_order(output[0])

        self.wired = {
            WiredIOArg.get_arg_name(i[0], i[1], i[2]): WiredIOArg.get_arg_name(o[0], o[1], o[2])
            for i, o in wired
        }

    def exec(self, inputs: dict) -> dict:
        args = list(inputs.keys())
        for arg in args:
            if isinstance(arg, WiredIOArg):
                if arg.func_idx is None:
                    func_idx = self.get_func_order(arg.func_id)
                else:
                    func_idx = arg.func_idx
                inputs[WiredIOArg.get_arg_name(arg.func_id, func_idx, arg.name)] = inputs.pop(arg)

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
                    print(
                        f"Error while wiring output of {func_cls} from {argname} to {WiredIOArg.get_arg_name(func_cls.id, self.idx2order[i], argname)}"
                    )
                    raise

        return output

    def validate(self, inputs: dict) -> bool:
        """
        Validate if the pipeline is correct
        :param self:
        :return:
        """
        return True

    def get_func_order(self, func_id: str) -> int:
        if len(self.id2order[func_id]) == 0:
            raise ValueError(f"Cannot wire argument to function {func_id} because it doesn't exist")

        if len(self.id2order[func_id]) > 1:
            raise ValueError(
                f"Cannot wire argument to function {func_id} because it is ambiguous (more than one function with same id)"
            )

        return self.id2order[func_id][0][-1]

    def save(self, save_path: Union[str, Path]):
        pass

    @staticmethod
    def load(load_path: Union[str, Path]):
        pass
