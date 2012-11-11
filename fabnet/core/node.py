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

from fabnet.core.fri_server import FriServer
from fabnet.settings import OPERATORS_MAP, DEFAULT_OPERATOR
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.core.key_storage import init_keystore
from fabnet.core.constants import ET_INFO
from fabnet.utils.logger import logger

class Node:
    def __init__(self, hostname, port, home_dir, node_name='anonymous_node',
                    ks_path=None, ks_passwd=None, node_type=None, bind_host='0.0.0.0'):
        self.hostname = hostname
        self.bind_host = bind_host
        self.port = port
        self.home_dir = home_dir
        self.node_name = node_name
        self.node_type = node_type
        if ks_path:
            self.keystore = init_keystore(ks_path, ks_passwd)
        else:
            self.keystore = None

        self.server = None

    def start(self, neighbour):
        address = '%s:%s' % (self.hostname, self.port)
        if not neighbour:
            is_init_node = True
        else:
            is_init_node = False

        operator_class = OPERATORS_MAP.get(self.node_type, None)
        if operator_class is None:
            logger.error('Node type "%s" does not found!'%self.node_type)
            return False

        operator = operator_class(address, self.home_dir, self.keystore, is_init_node, self.node_name)

        self.server = FriServer(self.bind_host, self.port, operator, \
                                    server_name=self.node_name, \
                                    keystorage=self.keystore)

        started = self.server.start()
        if not started:
            return started

        if is_init_node:
            return True

        packet = FabnetPacketRequest(method='DiscoveryOperation', sender=address)

        rcode, rmsg = self.server.operator.call_node(neighbour, packet)
        if rcode:
            logger.warning('Neighbour %s does not respond!'%neighbour)
            return False

        params = {'event_type': ET_INFO, 'event_topic': 'NodeUpDown', \
                'event_message': 'Hello, fabnet!', 'event_provider': address}
        packet = FabnetPacketRequest(method='NotifyOperation', parameters=params, sender=address)
        rcode, rmsg = self.server.operator.call_node(neighbour, packet)
        if rcode:
            logger.warning('Cant send notification to network. Details: %s'%rmsg)

        return True

    def stop(self):
        try:
            address = '%s:%s' % (self.hostname, self.port)
            params = {'event_type': ET_INFO, 'event_topic': 'NodeUpDown', \
                        'event_message': 'Goodbye, fabnet :(', 'event_provider': address}
            packet = FabnetPacketRequest(method='NotifyOperation', parameters=params, sender=address)
            rcode, rmsg = self.server.operator.call_network(packet)
            if rcode:
                raise Exception(rmsg)
        except Exception, err:
            logger.warning('Cant send notification to network. Details: %s'%err)

        return self.server.stop()
