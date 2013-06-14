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
BUF_SIZE = 100*1024

#default binary data chunk size
DEFAULT_CHUNK_SIZE = 1024*1024

#fri binary packet constants 
FRI_PROTOCOL_IDENTIFIER = 'FRI0'
FRI_PACKET_INFO_LEN = 20

#message container size
MC_SIZE = 10000

#fri responses container size
RQ_SIZE = 100

#response codes
RC_OK = 0
RC_ERROR = 1
RC_UNEXPECTED = -1
RC_DONT_STARTED = -2
RC_ALREADY_PROCESSED = -3
RC_MESSAGE_ID_NOT_FOUND = -4
RC_PERMISSION_DENIED = -5
RC_INVALID_CERT = -6
RC_NOT_MY_NEIGHBOUR = 22
RC_REQ_CERTIFICATE = 1010
RC_REQ_BINARY_CHUNK = 1011

#flag for worker stopping
STOP_WORKER_EVENT = None

#thread statuses
S_PENDING = -1
S_INWORK = 0
S_ERROR =  1

#neighbours types
NT_SUPERIOR = 1
NT_UPPER = 2

ONE_DIRECT_NEIGHBOURS_COUNT = 2

#keep alive constants
KEEP_ALIVE_METHOD = 'KeepAlive'
KEEP_ALIVE_TRY_COUNT = 3
KEEP_ALIVE_MAX_WAIT_TIME = 60
CHECK_NEIGHBOURS_TIMEOUT = 15

FRI_CLIENT_TIMEOUT = 10
FRI_CLIENT_READ_TIMEOUT = 120

WAIT_SYNC_OPERATION_TIMEOUT = 600

MIN_WORKERS_COUNT = 5
MAX_WORKERS_COUNT = 40


#certificates constantns
NODE_CERTIFICATE = 'nodes.idepositbox.com'
CLIENT_CERTIFICATE = 'clients.idepositbox.com'
NODE_ROLE = 'node'
CLIENT_ROLE = 'client'


# types
ET_ALERT = 'alert'
ET_INFO = 'info'

#unix socket for communication with Operator process
OPERATOR_SOCKET_ADDRESS = '/tmp/%s-fabnet-operator.socket'

STAT_COLLECTOR_TIMEOUT = 10
STAT_OSPROC_TIMEOUT = 20

#statistic objects
SO_OPERS_TIME = 'OperationsProcTime'
SI_SYS_INFO = 'SystemInfo'
SI_BASE_INFO = 'BaseInfo'
