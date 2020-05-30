#!/usr/bin/python
# -*- coding: utf-8 -*-
import copy
from datetime import datetime
from pathlib import Path
from typing import *
import ujson
from dateutil import parser


def dataset(val: Any, optional=False, preference: str = None, input_ref: str = None) -> 'ArgType':
    assert preference is None or preference == 'graph' or preference == 'array', 'preference only accepts values "graph" or "array"'
    return ArgType("dataset", optional=optional, val=val, preference=preference, input_ref=input_ref)


class ArgType(object):
    FilePath: 'ArgType' = None
    DataSet: Union[
        Callable[[Any], 'ArgType'],
        Callable[[Any, bool], 'ArgType'],
        Callable[[Any, bool, str], 'ArgType'],
        Callable[[Any, bool, str, str], 'ArgType']] = dataset
    OrderedDict: 'ArgType' = None
    String: 'ArgType' = None
    Number: 'ArgType' = None
    Boolean: 'ArgType' = None
    DateTime: 'ArgType' = None
    VarAggGroupBy: 'ArgType' = None
    VarAggFunc: 'ArgType' = None

    def __init__(self,
                 id: str,
                 optional: bool = False,
                 val: Any = None,
                 validate: Callable[[Any], bool] = lambda val: True,
                 from_str: Callable[[str], Any] = lambda val: val,
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

    def __call__(self, optional: bool = False):
        if optional == self.optional:
            return self
        o = copy.deepcopy(self.__dict__)
        o['optional'] = optional
        return ArgType(**o)

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


ArgType.FilePath = ArgType("file_path", validate=lambda val: Path(val).parent.exists(),
                           from_str=lambda val: str(Path(val)))
ArgType.OrderedDict = ArgType("ordered_dict", validate=lambda val: isinstance(val, dict))
ArgType.String = ArgType("string", validate=lambda val: isinstance(val, str))
ArgType.Number = ArgType("number", validate=lambda val: isinstance(val, int) or isinstance(val, float),
                         from_str=lambda val: ('.' in val and float(val)) or int(val))
ArgType.Boolean = ArgType("boolean", validate=lambda val: isinstance(val, bool),
                          from_str=lambda val: {'True': True, 'true': True, 'False': False, 'false': False}[val])
ArgType.DateTime = ArgType("datetime", validate=lambda val: isinstance(val, datetime),
                           from_str=lambda val: parser.parse(val))
ArgType.VarAggGroupBy = ArgType("var_agg_group_by",
                                validate=lambda val: isinstance(val, list),
                                from_str=lambda val: ujson.load(val))
ArgType.VarAggFunc = ArgType("var_agg_func",
                             validate=lambda val: val in {"sum", "average", "count"},
                             from_str=lambda val: val)
