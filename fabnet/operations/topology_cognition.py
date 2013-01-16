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

from fabnet.utils.db_conn import SqliteDBConnection as DBConnection
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
            conn = DBConnection(os.path.join(self.home_dir, TOPOLOGY_DB))

            conn.execute("CREATE TABLE IF NOT EXISTS fabnet_nodes(node_address TEXT, node_name TEXT, superiors TEXT, uppers TEXT, old_data INT)")
            conn.execute("UPDATE fabnet_nodes SET old_data=1")

            conn.close()

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

        conn = DBConnection(os.path.join(self.home_dir, TOPOLOGY_DB))

        self._lock()
        try:
            rows = conn.select("SELECT old_data FROM fabnet_nodes WHERE node_address='%s'" % node_address)
            if rows:
                conn.execute("UPDATE fabnet_nodes SET node_name='%s', superiors='%s', uppers='%s', old_data=0 WHERE node_address='%s'"% \
                    (node_name, ','.join(superior_neighbours), ','.join(upper_neighbours), node_address))
            else:
                conn.execute("INSERT INTO fabnet_nodes VALUES ('%s', '%s', '%s', '%s', 0)"% \
                        (node_address, node_name, ','.join(superior_neighbours), ','.join(upper_neighbours)))
        finally:
            self._unlock()
            conn.close()

        if packet.ret_parameters.get('need_rebalance', False):
            self.operator.smart_neighbours_rebalance(node_address, superior_neighbours, upper_neighbours)


