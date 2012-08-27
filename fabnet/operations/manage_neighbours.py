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
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER,\
                        ONE_DIRECT_NEIGHBOURS_COUNT
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.operations.constants import MNO_APPEND, MNO_REMOVE


class ManageNeighbour(OperationBase):
    def __init__(self, operator):
        OperationBase.__init__(self, operator)
        self.__cache = {}
        self.__cache[NT_UPPER] = []
        self.__cache[NT_SUPERIOR] = []

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
        upper_neighbours = self.operator.get_neighbours(NT_UPPER)
        superior_neighbours = self.operator.get_neighbours(NT_SUPERIOR)

        for_delete = None
        parameters = {'operation': MNO_REMOVE, 'node_address': self.operator.self_address}
        if len(superior_neighbours) > ONE_DIRECT_NEIGHBOURS_COUNT:
            for superior in superior_neighbours:
                if superior in self.__cache[NT_SUPERIOR]:
                    continue

                for_delete = superior
                if superior in upper_neighbours:
                    break
            else:
                self.__cache[NT_SUPERIOR] = []

            parameters['neighbour_type'] = NT_UPPER

        if for_delete is None and len(upper_neighbours) > ONE_DIRECT_NEIGHBOURS_COUNT:
            for upper in upper_neighbours:
                if upper in self.__cache[NT_UPPER]:
                    continue

                for_delete = upper
                if upper in upper_neighbours:
                    break
            else:
                self.__cache[NT_UPPER] = []

            parameters['neighbour_type'] = NT_SUPERIOR
        else:
            return

        if for_delete:
            self._init_operation(for_delete, 'ManageNeighbour', parameters)



    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        n_type, operation, node_address = self._valiadate_packet(packet.parameters)

        ret_params = packet.parameters
        ret_params['neighbour_type'] = n_type
        ret_params['node_address'] = self.operator.self_address

        if operation == MNO_APPEND:
            self.operator.set_neighbour(n_type, node_address)
        elif operation == MNO_REMOVE:
            neighbours = self.operator.get_neighbours(n_type)
            if len(neighbours) > ONE_DIRECT_NEIGHBOURS_COUNT:
                self.operator.remove_neighbour(n_type, node_address)
            else:
                ret_params['dont_remove'] = True

        if n_type == NT_SUPERIOR:
            n_type = NT_UPPER
        elif n_type == NT_UPPER:
            n_type = NT_SUPERIOR

        ret_params['neighbour_type'] = n_type

        #redirect response to connected/removed node
        packet.sender = node_address

        self._lock()
        try:
            self.rebalance_neighbours()
        finally:
            self._unlock()

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

        self._lock()
        try:
            if operation == MNO_APPEND:
                self.operator.set_neighbour(n_type, node_address)
            elif operation == MNO_REMOVE:
                dont_remove = packet.ret_parameters.get('dont_remove', False)
                if not dont_remove:
                    self.operator.remove_neighbour(n_type, node_address)
                else:
                    self.__cache[n_type].append(node_address)

            self.rebalance_neighbours()
        finally:
            self._unlock()

