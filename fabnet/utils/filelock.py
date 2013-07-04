#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.filelock
@author Konstantin Andrusenko
@date June 26, 2013
"""

import os
import fcntl

LOCK_EX = fcntl.LOCK_EX
LOCK_SH = fcntl.LOCK_SH
LOCK_NB = fcntl.LOCK_NB

class LockException(Exception):
    # Error codes:
    LOCK_FAILED = 1

class AlreadyExists(Exception):
    def __init__(self, path):
        Exception.__init__(self)
        self.path = path

    def __repr__(self):
        return 'File %s is already exists!'%self.path

    def __str__(self):
        return self.__repr__()


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


class LockedFile:
    def __init__(self, file_path, exclusive=False, new_file=False):
        self.__fpath = file_path
        self.__fd = None
        if exclusive:
            lock_type = LOCK_EX
        else:
            lock_type = LOCK_SH

        self.__open(lock_type, new_file)

    def __open(self, lock_type, create=False):
        if create:
            if os.path.exists(self.__fpath):
                raise AlreadyExists(self.__fpath)

            try:
                self.__fd = os.open(self.__fpath, os.O_CREAT|os.O_EXCL|os.O_RDWR)
            except OSError, err:
                if err[0] == 17: #[Errno 17] File exists
                    raise AlreadyExists(self.__fpath)
                else:
                    raise
        else:
            self.__fd = os.open(self.__fpath, os.O_RDWR)

        try:
            fcntl.flock(self.__fd, lock_type)
        except IOError, exc_value:
            #  IOError: [Errno 11] Resource temporarily unavailable
            if exc_value[0] == 11:
                raise LockException(LockException.LOCK_FAILED, exc_value[1])
            else:
                raise

    def write(self, data):
        os.write(self.__fd, data)

    def append(self, data):
        os.lseek(self.__fd, 0, os.SEEK_END) #go to end of file
        os.write(self.__fd, data)

    def read(self, rlen=None):
        if rlen is None:
            rlen = os.path.getsize(self.__fpath)
        return os.read(self.__fd, rlen)

    def seek(self, pos=0, how=0):
        os.fsync(self.__fd)
        os.lseek(self.__fd, pos, how)

    def close(self):
        if self.__fd:
            os.close(self.__fd)
            self.__fd = None

