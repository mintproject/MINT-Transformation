#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import *
from pathlib import Path
from collections import OrderedDict
from datetime import datetime


class ArgType(object):
    FilePath: 'ArgType' = None
    Graph: Callable[[Any], 'ArgType'] = lambda val: ArgType("graph", val=val)
    OrderedDict: 'ArgType' = None
    NDimArray: 'ArgType' = None
    String: 'ArgType' = None
    Number: 'ArgType' = None
    Boolean: 'ArgType' = None
    DateTime: 'ArgType' = None

    def __init__(self, id: str, optional: bool = False, val: Any = None, validate: Callable[[Any], bool] = lambda val: True):
        self.id = id
        self.val = val
        self.optional = optional
        self.validate = validate

    def __eq__(self, other):
        if other is None or not isinstance(other, ArgType):
            return False

        return self.id == other.id and self.val == other.val

    def __call__(self, optional: bool = False, val: Any = None):
        if optional == self.optional and val == self.val:
            return self
        return ArgType(self.id, optional, val)


def validate_file_path(value):
    try:
        str(Path(value))
        return True
    except TypeError:
        raise TypeError('Invalid Argument type. Expected FilePath')


def validate_ordered_dict(value):
    try:
        OrderedDict(value)
        return True
    except ValueError:
        raise TypeError('Invalid Argument type. Expected OrderedDict')


def validate_string(value):
    if isinstance(value, str):
        return True
    raise TypeError('Invalid Argument type. Expected String')


def validate_number(value):
    if isinstance(value, int) or isinstance(value, float):
        return True
    raise TypeError('Invalid Argument type. Expected Number')


def validate_boolean(value):
    if isinstance(value, bool):
        return True
    raise TypeError('Invalid Argument type. Expected Boolean')


def validate_datetime(value):
    if isinstance(value, datetime):
        return True
    raise TypeError('Invalid Argument type. Expected DateTime')


ArgType.FilePath = ArgType("file_path", validate=validate_file_path)
ArgType.OrderedDict = ArgType("ordered_dict", validate=validate_ordered_dict)
ArgType.NDimArray = ArgType("ndim_array")
ArgType.String = ArgType("string", validate=validate_string)
ArgType.Number = ArgType("number", validate=validate_number)
ArgType.Boolean = ArgType("boolean", validate=validate_boolean)
ArgType.DateTime = ArgType("datetime", validate=validate_datetime)
