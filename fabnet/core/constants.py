#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.constants
@author Konstantin Andrusenko
@date August 22, 2012

This module contains the fabnet core constants
"""

#socket buffer size
BUF_SIZE = 1024

#message container size
MC_SIZE = 10000

#response codes
RC_OK = 0
RC_ERROR = 1
RC_UNEXPECTED = -1

#flag for worker stopping
STOP_THREAD_EVENT = None

#thread statuses
S_PENDING = -1
S_INWORK = 0
S_ERROR =  1

#keep alive packet marker
KEEP_ALIVE_PACKET = 'KEEP-ALIVE'

#neighbours types
NT_SUPERIOR = 1
NT_UPPER = 2

ONE_DIRECT_NEIGHBOURS_COUNT = 2
