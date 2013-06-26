#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.safe_json_file
@author Konstantin Andrusenko
@date June 26, 2013
"""
import json
import os
from fabnet.utils.filelock import *

class SafeJsonFile:
    def __init__(self, file_path):
        self.__fpath = file_path
        if not os.path.exists(self.__fpath):
            open(self.__fpath, 'w').close()

    def read(self):
        if not os.path.exists(self.__fpath):
            return {}

        f = open(self.__fpath)
        lock(f, LOCK_SH)
        try:
            data = f.read() 
        finally:
            f.close()

        if not data:
            return {}
        return json.loads(data)


    def write(self, data):
        j_str = json.dumps(data)
        f = open(self.__fpath, 'r+')
        lock(f, LOCK_EX)
        try:
            f.truncate()
            data = f.write(j_str) 
        finally:
            f.close()



