#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.discovery_operation

@author Konstantin Andrusenko
@date September 7, 2012
"""
from fabnet.core.operator_base import  OperationBase
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER,\
                        ONE_DIRECT_NEIGHBOURS_COUNT
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.operations.constants import MNO_APPEND

class DiscoveryOperation(OperationBase):
    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        pass

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        neighbours = [self.operator.self_address] + \
                        self.operator.get_neighbours(NT_UPPER) + \
                        self.operator.get_neighbours(NT_SUPERIOR)
        count = ONE_DIRECT_NEIGHBOURS_COUNT
        if count > len(neighbours):
            count = len(neighbours)

        node_type = NT_UPPER
        for i in xrange(count):
            parameters = { 'neighbour_type': node_type, 'operation': MNO_APPEND,
                        'node_address': neighbours[i] }
            self._init_operation(packet.sender, 'ManageNeighbour', parameters)

        neighbours.reverse()
        node_type = NT_SUPERIOR
        for i in xrange(count):
            parameters = { 'neighbour_type': node_type, 'operation': MNO_APPEND,
                        'node_address': neighbours[i] }
            self._init_operation(packet.sender, 'ManageNeighbour', parameters)

        return FabnetPacketResponse()


    def callback(self, packet, sender=None):
        """In this method should be implemented logic of processing
        response packet from requested node

        @param packet - object of FabnetPacketResponse class
        @param sender - address of sender node.
        If sender == None then current node is operation initiator
        @return object of FabnetPacketResponse
                that should be resended to current node requestor
                or None for disabling packet resending
        """
        pass
