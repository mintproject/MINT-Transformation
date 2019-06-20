#!/usr/bin/python
# -*- coding: utf-8 -*-
import abc
from typing import Dict
from dtran.argtype import ArgType


class IFunc(abc.ABC):
    id: str = ""
    inputs: Dict[str, ArgType] = {}
    outputs: Dict[str, ArgType] = {}

    @abc.abstractmethod
    def validate(self) -> bool:
        """
        Check if the inputs are correct or not
        :return:
        """
        pass

    @abc.abstractmethod
    def exec(self) -> dict:
        """
        Execute the transformation function and return the result
        :return:
        """
        raise NotImplementedError()
