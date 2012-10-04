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
from fabnet.settings import OPERATOR, OPERATIONS_MAP
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.utils.logger import logger

class Node:
    def __init__(self, hostname, port, home_dir, node_name='anonymous_node', certfile=None, keyfile=None):
        self.hostname = hostname
        self.port = port
        self.home_dir = home_dir
        self.node_name = node_name
        self.certfile = certfile
        self.keyfile = keyfile

        self.server = None

    def start(self, neighbour):
        address = '%s:%s' % (self.hostname, self.port)
        if not neighbour:
            is_init_node = True
        else:
            is_init_node = False
        operator = OPERATOR(address, self.home_dir, self.certfile, is_init_node, self.node_name)

        for (op_name, op_class) in OPERATIONS_MAP.items():
            operator.register_operation(op_name, op_class)

        self.server = FriServer(self.hostname, self.port, operator, \
                                    server_name=self.node_name, \
                                    certfile=self.certfile, \
                                    keyfile=self.keyfile)

        started = self.server.start()
        if not started:
            return started

        if is_init_node:
            return True

        packet = FabnetPacketRequest(method='DiscoveryOperation', sender=address)

        rcode, rmsg = self.server.operator.call_node(neighbour, packet)
        if rcode:
            logger.warning('Neighbour %s does not respond!'%neighbour)

        return True

    def stop(self):
        return self.server.stop()
