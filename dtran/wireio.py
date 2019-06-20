#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import Dict, Optional

from dtran import ArgType


class WiredIO:

    def __init__(self, io: str, func_id: str, props: Dict[str, ArgType], func_idx=None):
        self.func_id = func_id
        self.io = io
        self.props = props
        self.func_idx = func_idx

    def __getattr__(self, attr):
        if self.func_idx is None:
            if attr[0] == "_" and attr[1:].isdigit():
                return WiredIO(self.io, self.func_id, self.props, int(attr[1:]))
            return WiredIOArg(self.func_id, None, attr, self.io, self.props[attr])

        if attr in self.props:
            return WiredIOArg(self.func_id, self.func_idx, attr, self.io, self.props[attr])

        raise AttributeError(f"Attribute {attr} not found")


class WiredIOArg:

    @staticmethod
    def get_arg_name(id: str, idx: int, argname: str):
        return f"{id}__{idx}__{argname}"

    def __init__(self, func_id: str, func_idx: Optional[int], name: str, io: str, argtype: ArgType):
        self.func_idx = func_idx
        self.func_id = func_id
        self.io = io
        self.name = name
        self.argtype = argtype

    def __eq__(self, other):
        if other is None or not isinstance(other, WiredIOArg):
            raise ValueError(other)

        if self.argtype != other.argtype and self.io != other.io:
            raise ValueError("Cannot wire two arguments because they are not compatible")

        if self.io == "i":
            return [self.func_id, self.func_idx, self.name], [other.func_id, other.func_idx, other.name]
        else:
            return [other.func_id, other.func_idx, other.name], [self.func_id, self.func_idx, self.name]

    def __hash__(self):
        return hash((self.func_id, self.func_idx, self.io, self.name))

    def __str__(self):
        if self.func_idx is None:
            raise ValueError("Cannot convert WiredIOArg to string because the index of the function is null")
        return self.get_arg_name(self.func_id, self.func_idx, self.name)
