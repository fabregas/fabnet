#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.node
@author Konstantin Andrusenko
@date September 7, 2012

This module contains the Node class implementation
"""

from fabnet.core.fri_base import FriServer
from fabnet.core.operator_base import Operator
from fabnet.operations import OPERATIONS_MAP

class Node:
    def __init__(self, hostname, port, home_dir, node_name='anonymous_node'):
        self.hostname = hostname
        self.port = port
        self.home_dir = home_dir
        self.node_name = node_name

        self.server = None

    def start(self):
        operator = Operator('%s:%s'%(self.hostname, self.port), self.home_dir)

        for (op_name, op_class) in OPERATIONS_MAP.items():
            operator.register_operation(op_name, op_class)

        self.server = FriServer(self.hostname, self.port, operator, \
                                    server_name=self.node_name)

        return server.start()

    def stop(self):
        return server.stop()
