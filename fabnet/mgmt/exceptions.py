#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.mgmt.exceptions
@author Konstantin Andrusenko
@date July 24, 2013
"""

class MEBaseException(Exception):
    ERR_UNKN = 1
    ERR_AUTH = 10
    ERR_PERM = 11
    ERR_OPER = 20
    ERR_UNEX = 30
    ERR_DBER = 40
    ERR_NFND = 50
    ERR_ALEX = 55
    ERR_INAR = 60

    def __init__(self, ret_code, msg):
        self.ret_code = ret_code
        self.msg = msg

    def __repr__(self):
        return '[%s] %s'%(self.ret_code, self.msg)

    def __str__(self):
        return self.__repr__()

    def __unicode__(self):
        return self.__repr__()

class MEAuthException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_AUTH, msg)

class MEPermException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_PERM, msg)

class MEOperException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_OPER, msg)

class MEUnexpException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_UNEX, msg)

class MEDatabaseException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_DBER, msg)

class MENotFoundException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_NFND, msg)

class MEAlreadyExistsException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_ALEX, msg)

class MEInvalidArgException(MEBaseException):
    def __init__(self, msg):
        MEBaseException.__init__(self, self.ERR_INAR, msg)
