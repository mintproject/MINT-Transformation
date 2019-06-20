#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import *


class ArgType(object):
    FilePath: 'ArgType' = None
    Graph: 'Callable[[None], ArgType]' = None
    NDimArray: 'ArgType' = None
    String: 'ArgType' = None
    Number: 'ArgType' = None
    Boolean: 'ArgType' = None
    DateTime: 'ArgType' = None

    def __init__(self, id: str, val: Any = None):
        self.id = id
        self.val = val

    def __eq__(self, other):
        if other is None or not isinstance(other, ArgType):
            return False

        return self.id == other.id and self.val == other.val


ArgType.FilePath = ArgType("file_path")
ArgType.Graph = lambda x: ArgType("graph", x)
ArgType.NDimArray = ArgType("ndim_array")
ArgType.String = ArgType("string")
ArgType.Number = ArgType("number")
ArgType.Boolean = ArgType("boolean")
ArgType.DateTime = ArgType("datetime")
