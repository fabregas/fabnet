#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.config
@author Konstantin Andrusenko
@date November 15, 2012
"""
import os
import threading
import copy

class ConfigAttrs(type):
    __params = {}
    __lock = threading.RLock()
    __config_file = None

    @classmethod
    def update_config(cls, new_config, section=None, nosave=False):
        cls.__lock.acquire()
        try:
            if section:
                conf = cls.__params.get(section, {})
                conf.update(new_config)
                cls.__params[section] = conf
            else:
                for key, value in new_config.items():
                    if not isinstance(value, dict):
                        raise Exception('Invalid config structure at %s'%key)
                    if key in cls.__params:
                        cls.__params[key].update(value)
                    else:
                        cls.__params[key] = value


            if not nosave:
                cls.save()
        finally:
            cls.__lock.release()

    @classmethod
    def get_config_dict(cls, section=None, default=None):
        cls.__lock.acquire()
        try:
            if section:
                return copy.copy(cls.__params.get(section, default))
            return copy.copy(cls.__params)
        finally:
            cls.__lock.release()

    def __getattr__(cls, attr):
        return cls.get(attr)

    @classmethod
    def get(cls, attr, section=None):
        cls.__lock.acquire()
        try:
            if section:
                return cls.__params.get(section, {}).get(attr, None)
            if attr in cls.__params:
                return cls.__params[attr]
            for section, config in cls.__params.items():
                if isinstance(config, dict) and attr in config:
                    return config[attr]
            return None
        finally:
            cls.__lock.release()

    @classmethod
    def load(cls, config_file):
        cls.__config_file = config_file
        if not os.path.exists(config_file):
            return

        import yaml
        r_str = open(config_file).read()
        data = yaml.load(r_str)
        cls.update_config(data, nosave=True)

    @classmethod
    def save(cls):
        if not cls.__config_file:
            return

        import yaml 
        r_str = yaml.dump(cls.__params, default_flow_style=False)
        open(cls.__config_file, 'w').write(r_str)


class Config(object):
    __metaclass__ = ConfigAttrs

