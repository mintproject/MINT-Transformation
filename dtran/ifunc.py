#!/usr/bin/python
# -*- coding: utf-8 -*-
import abc
from typing import Dict, Optional
from dtran.argtype import ArgType
from dtran.wireio import WiredIO


class IFuncIO(type):

    # noinspection PyPep8Naming, PyMethodParameters,PyUnresolvedReferences
    @property
    def I(cls):
        return WiredIO("i", cls.id, cls.inputs)

    # noinspection PyPep8Naming, PyMethodParameters,PyUnresolvedReferences
    @property
    def O(cls):
        return WiredIO("o", cls.id, cls.outputs)


class IFunc(metaclass=IFuncIO):
    id: str = ""
    description = None
    inputs: Dict[str, ArgType] = {}
    outputs: Dict[str, ArgType] = {}

    @abc.abstractmethod
    def validate(self) -> bool:
        """
        Check if the inputs are correct or not
        :return:
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def exec(self) -> dict:
        """
        Execute the transformation function and return the result
        :return:
        """
        raise NotImplementedError()

    @staticmethod
    def filter_func(filter):
        if filter is None:
            return lambda n: True
        # rewrite the query
        conditions = []
        for and_expr in filter.split(" and "):
            and_expr = and_expr.strip()
            if " = " in and_expr:
                field, value = and_expr.split(" = ")
                conditions.append(f'(n.data["{field}"] == {value})')
            elif ".contains(" in and_expr:
                field, value = and_expr.split(".contains(")
                conditions.append(f'(n.data["{field}"].find({value[:-1]}) != -1)')
            elif " in " in and_expr:
                field, value = and_expr.split(" in ")
                conditions.append(f'(n.data["{field}"] in {value})')
            else:
                raise NotImplementedError(f"Doesn't handle {and_expr} yet")

        return eval(f"lambda n: " + " and ".join(conditions))

    def set_preferences(self, preferences: Dict[str, str]) -> None:
        self.preferences = preferences

    def get_preference(self, output: str) -> str:
        return self.preferences[output]
