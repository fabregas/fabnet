#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.config
@author Konstantin Andrusenko
@date November 15, 2012
"""
import threading
import copy

class ConfigAttrs(type):
    __params = {}
    __lock = threading.Lock()

    @classmethod
    def update_config(cls, new_config):
        cls.__lock.acquire()
        try:
            cls.__params.update(new_config)
        finally:
            cls.__lock.release()

    @classmethod
    def get_config_dict(cls):
        cls.__lock.acquire()
        try:
            return copy.copy(cls.__params)
        finally:
            cls.__lock.release()

    def __getattr__(cls, attr):
        cls.__lock.acquire()
        try:
            return cls.__params.get(attr, None)
        finally:
            cls.__lock.release()


class Config(object):
    __metaclass__ = ConfigAttrs


