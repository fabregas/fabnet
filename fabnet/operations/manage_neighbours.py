#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.manage_meighbours

@author Konstantin Andrusenko
@date September 7, 2012
"""

from fabnet.core.operation_base import OperationBase
from fabnet.core.constants import NT_SUPERIOR, NT_UPPER, \
                        ONE_DIRECT_NEIGHBOURS_COUNT
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.operations.constants import MNO_APPEND, MNO_REMOVE
from fabnet.utils.logger import logger
from fabnet.operations.topology_cognition import TopologyCognition


class ManageNeighbour(OperationBase):
    def __init__(self, operator):
        OperationBase.__init__(self, operator)
        self.__cache = {}
        self.__cache[NT_UPPER] = []
        self.__cache[NT_SUPERIOR] = []
        self.__discovered_nodes = {}
        self.__discovered_nodes[NT_UPPER] = []
        self.__discovered_nodes[NT_SUPERIOR] = []

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

    def _check_neighbours_count(self, n_type, neighbours, other_n_type, other_neighbours, ret_parameters):
        self._lock()
        try:
            if len(neighbours) >= ONE_DIRECT_NEIGHBOURS_COUNT:
                self.__discovered_nodes[n_type] = []
                return

            new_node = None
            for node in ret_parameters.get('neighbours', []):
                #trying find new node...
                if (node in self.__discovered_nodes[n_type]) or (node in neighbours) \
                        or (node in other_neighbours) or (node == self.operator.self_address):
                    continue
                new_node = node
                break

            d_nodes = TopologyCognition(self.operator).get_discovered_nodes()
            for node_addr, node_info in d_nodes.items():
                if (node_addr in self.__discovered_nodes[n_type]) or (node_addr in neighbours) \
                        or (node_addr in other_neighbours) or (node_addr == self.operator.self_address):
                    continue
                new_node = node_addr
                if len(set(node_info[0]) & set(node_info[1])):
                    #node with interset neighbours
                    break

            if new_node == None:
                for node in neighbours:
                    if node not in self.__discovered_nodes[n_type]:
                        new_node = node
                        break

            if new_node == None:
                for node in other_neighbours:
                    if node not in self.__discovered_nodes[n_type]:
                        new_node = node
                        break

            if new_node is None:
                return
        finally:
            self._unlock()

        parameters = { 'neighbour_type': other_n_type, 'operation': MNO_APPEND,
                    'node_address': self.operator.self_address }
        ret_code, msg = self._init_operation(new_node, 'ManageNeighbour', parameters)
        if ret_code:
            self.__discovered_nodes[n_type].append(new_node)
            self._check_neighbours_count(n_type, neighbours, other_n_type, other_neighbours, ret_parameters)


    def rebalance_append(self, ret_parameters):
        upper_neighbours = self.operator.get_neighbours(NT_UPPER)
        superior_neighbours = self.operator.get_neighbours(NT_SUPERIOR)

        if ret_parameters['neighbour_type'] == NT_UPPER:
            self._check_neighbours_count(NT_UPPER, upper_neighbours, NT_SUPERIOR, superior_neighbours, ret_parameters)
        else:
            self._check_neighbours_count(NT_SUPERIOR, superior_neighbours, NT_UPPER, upper_neighbours, ret_parameters)


    def _get_for_delete(self, neighbours, n_type, other_neighbours):
        if len(neighbours) <= ONE_DIRECT_NEIGHBOURS_COUNT:
            return

        intersec_neighbours = list(set(neighbours) & set(other_neighbours))

        for neighbour in intersec_neighbours:
            if neighbour in self.__cache[n_type]:
                continue

            self.__cache[n_type].append(neighbour)
            return neighbour

        rest_neighbours = list(set(neighbours) ^ set(intersec_neighbours))
        for neighbour in rest_neighbours:
            if neighbour in self.__cache[n_type]:
                continue
            self.__cache[n_type].append(neighbour)
            return neighbour

        return None


    def rebalance_remove(self):
        upper_neighbours = self.operator.get_neighbours(NT_UPPER)
        superior_neighbours = self.operator.get_neighbours(NT_SUPERIOR)

        self._lock()
        try:
            parameters = {'operation': MNO_REMOVE, 'node_address': self.operator.self_address}

            for_delete = self._get_for_delete(superior_neighbours, NT_SUPERIOR, upper_neighbours)
            if for_delete:
                parameters['neighbour_type'] = NT_UPPER
            else:
                for_delete = self._get_for_delete(superior_neighbours, NT_UPPER, upper_neighbours)
                parameters['neighbour_type'] = NT_SUPERIOR
        finally:
            self._unlock()

        if for_delete:
            self._init_operation(for_delete, 'ManageNeighbour', parameters)
        else:
            self.__cache[NT_UPPER] = []
            self.__cache[NT_SUPERIOR] = []


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        n_type, operation, node_address = self._valiadate_packet(packet.parameters)
        is_force = packet.parameters.get('force', False)

        ret_params = packet.parameters

        neighbours = self.operator.get_neighbours(n_type)
        if operation == MNO_APPEND:
            if len(neighbours) >= (ONE_DIRECT_NEIGHBOURS_COUNT+1):
                ret_params['dont_append'] = True
            else:
                self.operator.set_neighbour(n_type, node_address)

            self.__discovered_nodes[n_type].append(node_address)
        elif operation == MNO_REMOVE:
            if (len(neighbours)  > ONE_DIRECT_NEIGHBOURS_COUNT) or is_force:
                self.operator.remove_neighbour(n_type, node_address)
            else:
                ret_params['dont_remove'] = True

        if is_force:
            self._lock()
            self.__discovered_nodes[n_type].append(node_address)
            self._unlock()
            return

        if operation == MNO_APPEND:
            r_neighbours = self.operator.get_neighbours(NT_UPPER) + \
                                        self.operator.get_neighbours(NT_SUPERIOR)
            ret_params['neighbours'] = dict(zip(r_neighbours, [0 for i in r_neighbours])).keys()

        if n_type == NT_SUPERIOR:
            n_type = NT_UPPER
        elif n_type == NT_UPPER:
            n_type = NT_SUPERIOR

        ret_params['node_address'] = self.operator.self_address
        ret_params['neighbour_type'] = n_type

        return FabnetPacketResponse(ret_parameters=ret_params)

    def after_process(self, packet, ret_packet):
        """In this method should be implemented logic that should be
        executed after response send

        @param packet - object of FabnetPacketRequest class
        @param ret_packet - object of FabnetPacketResponse class
        """
        if ret_packet:
            if packet.parameters.get('operation', MNO_APPEND) == MNO_APPEND \
                    and ret_packet.ret_parameters.get('dont_append', False) == False:
                self.rebalance_remove()

            if packet.parameters.get('operation', MNO_APPEND) == MNO_REMOVE \
                    and ret_packet.ret_parameters.get('dont_remove', False) == True:
                self.rebalance_remove()

        self.rebalance_append(packet.parameters)


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
                self.__discovered_nodes[n_type].append(node_address)
                dont_append = packet.ret_parameters.get('dont_append', False)
                if not dont_append:
                    self.operator.set_neighbour(n_type, node_address)

            elif operation == MNO_REMOVE:
                dont_remove = packet.ret_parameters.get('dont_remove', False)
                if not dont_remove:
                    self.operator.remove_neighbour(n_type, node_address)
                else:
                    self.__cache[n_type].append(node_address)
        finally:
            self._unlock()

        self.rebalance_remove()
        self.rebalance_append(packet.ret_parameters)



