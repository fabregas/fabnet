#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.filelock
@author Konstantin Andrusenko
@date June 26, 2013
"""
__all__ = [
    "lock",
    "unlock",
    "LOCK_EX",
    "LOCK_SH",
    "LOCK_NB",
    "LockException",
    "LockFile"
]

import os

class LockException(Exception):
    # Error codes:
    LOCK_FAILED = 1

import fcntl
LOCK_EX = fcntl.LOCK_EX
LOCK_SH = fcntl.LOCK_SH
LOCK_NB = fcntl.LOCK_NB

def lock(file, flags):
    try:
        fcntl.flock(file.fileno(), flags)
    except IOError, exc_value:
        #  IOError: [Errno 11] Resource temporarily unavailable
        if exc_value[0] == 11:
            raise LockException(LockException.LOCK_FAILED, exc_value[1])
        else:
            raise

def unlock(file):
    fcntl.flock(file.fileno(), fcntl.LOCK_UN)


class LockFile:
    def __init__(self, file_path):
        self.__fpath = '%s.lock'%file_path
        self.__f = None

    def ex_lock(self):
        self.__f = open(self.__fpath, 'w')
        lock(self.__f, LOCK_EX)

    def sh_lock(self):
        self.__f = open(self.__fpath, 'w')
        lock(self.__f, LOCK_SH)

    def unlock(self):
        if self.__f:
            unlock(self.__f)
            self.__f.close()
            self.__f = None
            try:
                os.remove(self.__fpath)
            except OSError:
                pass
