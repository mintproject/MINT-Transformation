#!/usr/bin/python
# -*- coding: utf-8 -*-


# noinspection PyPep8Naming
class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)
