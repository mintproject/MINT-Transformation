#!/usr/bin/python
# -*- coding: utf-8 -*-

from typing import *

from dtran.ifunc import IFunc


class Pipeline(object):

    def __init__(self, func_cls: List[Type[IFunc]]):
        self.func_cls = func_cls

    def exec(self, inputs: dict) -> dict:
        output = {}

        for func_cls in self.func_cls:
            func_args = {}
            for argname in func_cls.inputs.keys():
                func_args[argname] = inputs[f"{func_cls.id}_{argname}"]

            func = func_cls(**func_args)
            result = func.exec()
            for argname in func_cls.outputs.keys():
                output[f"{func_cls.id}_{argname}"] = result[argname]

        return output

    def validate(self) -> bool:
        """
        Validate if the pipeline is correct
        :param self:
        :return:
        """
        return True
