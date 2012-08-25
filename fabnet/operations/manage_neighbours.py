#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.manage_meighbours

@author Konstantin Andrusenko
@date September 7, 2012
"""

from fabnet.core.operator_base import OperationBase
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.operations.constants import MNO_APPEND, MNO_REMOVE

class ManageNeighbour(OperationBase):
    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        pass

    def _valiadate_packet(self, parameters):
        n_type = parameters.get('neighbour_type', None)
        if n_type is None:
            raise Exception('Neighbour type parameter is expected for ManageNeighbour operation')

        operation = parameters.get('operation', None)
        if operation is None:
            raise Exception('Operation parameter is expected for ManageNeighbour operation')

        node_address = parameters.get('node_address', None)
        if node_address is None:
            raise Exception('Node address parameter is expected for ManageNeighbour operation')

        return n_type, operation, node_address

    def rebalance_neighbours(self):
        pass

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        n_type, operation, node_address = self._valiadate_packet(packet.parameters)

        if operation == MNO_APPEND:
            self.operator.set_neighbour(n_type, node_address)
        elif operation == MNO_REMOVE:
            self.operator.remove_neighbour(n_type, node_address)

        if n_type == NT_SUPERIOR:
            n_type = NT_UPPER
        elif n_type == NT_UPPER:
            n_type = NT_SUPERIOR

        ret_params = packet.parameters
        ret_params['neighbour_type'] = n_type
        ret_params['node_address'] = self.operator.self_address

        self.rebalance_neighbours()

        return FabnetPacketResponse(ret_parameters=ret_params)


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
        n_type, operation, node_address = self._valiadate_packet(packet.ret_parameters)

        if operation == MNO_APPEND:
            self.operator.set_neighbour(n_type, node_address)
        elif operation == MNO_REMOVE:
            self.operator.remove_neighbour(n_type, node_address)

        self.rebalance_neighbours()
