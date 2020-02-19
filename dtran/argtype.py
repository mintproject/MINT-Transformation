#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import *
from pathlib import Path
from datetime import datetime
from dateutil import parser


def dataset(val: Any, preference: str = None, input_ref: str = None) -> 'ArgType':
    assert preference is None or preference == 'graph' or preference == 'array', 'preference only accepts values "graph" or "array"'
    return ArgType("dataset", val=val, preference=preference, input_ref=input_ref)


class ArgType(object):
    FilePath: 'ArgType' = None
    DataSet: Callable[[Any, str, str], 'ArgType'] = dataset
    OrderedDict: 'ArgType' = None
    String: 'ArgType' = None
    Number: 'ArgType' = None
    Boolean: 'ArgType' = None
    DateTime: 'ArgType' = None

    def __init__(self, id: str, optional: bool = False, val: Any = None,
                 validate: Callable[[Any], bool] = lambda val: True, from_str: Callable[[str], Any] = lambda val: val,
                 **kwargs):
        self.id = id
        self.val = val
        self.optional = optional
        self.validate = validate
        self.from_str = from_str
        vars(self).update(kwargs)

    def __eq__(self, other):
        if other is None or not isinstance(other, ArgType):
            return False

        return self.id == other.id and self.val == other.val

    def __call__(self, optional: bool = False, val: Any = None):
        if optional == self.optional and val == self.val:
            return self
        return ArgType(self.id, optional, val)

    def is_valid(self, val: Any):
        try:
            return self.validate(val)
        except (TypeError, ValueError):
            return False

    def type_cast(self, val: str):
        if not isinstance(val, str):
            raise NotImplementedError('type_casting is only supported from string type currently')
        try:
            return self.from_str(val)
        except (TypeError, ValueError, KeyError):
            raise ValueError(f"could not convert string to {self.id}")


ArgType.FilePath = ArgType("file_path", validate=lambda val: Path(val).parent.exists(), from_str=lambda val: str(Path(val)))
ArgType.OrderedDict = ArgType("ordered_dict", validate=lambda val: isinstance(val, dict))
ArgType.String = ArgType("string", validate=lambda val: isinstance(val, str))
ArgType.Number = ArgType("number", validate=lambda val: isinstance(val, int) or isinstance(val, float),
                         from_str=lambda val: ('.' in val and float(val)) or int(val))
ArgType.Boolean = ArgType("boolean", validate=lambda val: isinstance(val, bool),
                          from_str=lambda val: {'True': True, 'true': True, 'False': False, 'false': False}[val])
ArgType.DateTime = ArgType("datetime", validate=lambda val: isinstance(val, datetime),
                           from_str=lambda val: parser.parse(val))
