#!/usr/bin/python
# -*- coding: utf-8 -*-
import abc
from enum import Enum
from typing import Dict, Optional, Union, Generator, AsyncGenerator

from dtran.argtype import ArgType
from dtran.metadata import Metadata
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


class IFuncType(Enum):
    READER = "Reader"
    WRITER = "Writer"
    OTHERS = "Others"
    SPATIAL_TRANS = "Spatial Transformation"
    UNIT_TRANS = "Unit Conversion Transformation"
    MODEL_TRANS = "Model-specific Transformation"
    CROPPING_TRANS = "Cropping Transformation"
    AGGREGATION_TRANS = "Aggregation Transformation"
    INTERMEDIATE = "Other Transformation"


class IFunc(metaclass=IFuncIO):
    id: str = ""
    description = None
    inputs: Dict[str, ArgType] = {}
    outputs: Dict[str, ArgType] = {}
    preferences: Dict[str, str] = {}

    @abc.abstractmethod
    def validate(self) -> bool:
        """
        Check if the inputs are correct or not
        :return:
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def exec(self) -> Union[dict, Generator[dict, None, None], AsyncGenerator[dict, None]]:
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
        for output in self.outputs.keys() - preferences:
            if self.outputs[output].id == 'dataset':
                self.preferences[output] = None

    def get_preference(self, output: str) -> Optional[str]:
        return self.preferences[output]

    @abc.abstractmethod
    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        pass
