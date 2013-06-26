#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.topology_cognition

@author Konstantin Andrusenko
@date September 7, 2012
"""
import os
import threading
from datetime import datetime

from fabnet.utils.safe_json_file import SafeJsonFile
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import NT_SUPERIOR, NT_UPPER, \
                        ONE_DIRECT_NEIGHBOURS_COUNT, \
                        NODE_ROLE, CLIENT_ROLE
from fabnet.operations.constants import MNO_APPEND
from fabnet.utils.logger import oper_logger as logger

TOPOLOGY_DB = 'fabnet_topology.db'

class TopologyCognition(OperationBase):
    ROLES = [NODE_ROLE]

    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        if packet.sender is None:
            db = SafeJsonFile(os.path.join(self.home_dir, TOPOLOGY_DB))
            data = db.read()
            if data:
                for item in data.values():
                    item['old_data'] = 1
                db.write(data)

        return packet

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        ret_params = {}
        upper_neighbours = self.operator.get_neighbours(NT_UPPER)
        superior_neighbours = self.operator.get_neighbours(NT_SUPERIOR)

        ret_params.update(packet.parameters)
        ret_params['node_address'] = self.self_address
        ret_params['node_name'] = self.operator.get_node_name()
        ret_params['home_dir'] = self.operator.get_home_dir()
        ret_params['node_type'] = self.operator.get_type()
        ret_params['upper_neighbours'] = upper_neighbours
        ret_params['superior_neighbours'] = superior_neighbours

        return FabnetPacketResponse(ret_parameters=ret_params)


    def callback(self, packet, sender):
        """In this method should be implemented logic of processing
        response packet from requested node

        @param packet - object of FabnetPacketResponse class
        @param sender - address of sender node.
        If sender == None then current node is operation initiator

        @return object of FabnetPacketResponse
                that should be resended to current node requestor
                or None for disabling packet resending
        """
        if sender:
            return packet

        node_address = packet.ret_parameters.get('node_address', None)
        superior_neighbours = packet.ret_parameters.get('superior_neighbours', None)
        upper_neighbours = packet.ret_parameters.get('upper_neighbours', None)
        node_name = packet.ret_parameters.get('node_name', '')

        if (node_address is None) or (superior_neighbours is None) or (upper_neighbours is None):
            raise Exception('TopologyCognition response packet is invalid! Packet: %s'%str(packet.to_dict()))

        self._lock()
        try:
            db = SafeJsonFile(os.path.join(self.home_dir, TOPOLOGY_DB))
            data = db.read()
            data[node_address] = {'node_name': node_name, 'superiors': superior_neighbours, \
                    'uppers': upper_neighbours, 'old_data': 0}
            db.write(data)
        finally:
            self._unlock()

        if packet.ret_parameters.get('need_rebalance', False):
            self.operator.smart_neighbours_rebalance(node_address, superior_neighbours, upper_neighbours)


