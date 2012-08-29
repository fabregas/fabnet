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
import yaml
import sqlite3

from fabnet.core.operator_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER
from fabnet.utils.logger import logger


TOPOLOGY_DB = 'fabnet_topology.db'

class TopologyCognition(OperationBase):
    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        if packet.sender is None:
            conn = sqlite3.connect(os.path.join(self.operator.home_dir, TOPOLOGY_DB))

            curs = conn.cursor()
            curs.execute("DROP TABLE IF EXISTS fabnet_nodes")
            curs.execute("CREATE TABLE fabnet_nodes(node_address TEXT, node_name TEXT, superiors TEXT, uppers TEXT)")
            conn.commit()

            curs.close()
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

        ret_params['node_address'] = self.operator.self_address
        ret_params['node_name'] = self.operator.node_name
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

        conn = sqlite3.connect(os.path.join(self.operator.home_dir, TOPOLOGY_DB))

        curs = conn.cursor()
        curs.execute("INSERT INTO fabnet_nodes VALUES ('%s', '%s', '%s', '%s')"% \
                (node_address, node_name, ','.join(superior_neighbours), ','.join(upper_neighbours)))
        conn.commit()

        curs.close()
        conn.close()

