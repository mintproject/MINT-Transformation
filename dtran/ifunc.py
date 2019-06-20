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
