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
                        NODE_ROLE, CLIENT_ROLE, RC_OK, RC_ERROR
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.operations.constants import MNO_APPEND, MNO_REMOVE
from fabnet.utils.logger import logger
from fabnet.operations.topology_cognition import TopologyCognition


class ManageNeighbour(OperationBase):
    ROLES = [NODE_ROLE]

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

        op_type = parameters.get('operator_type', None)

        return n_type, operation, node_address, op_type


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        if self.operator.is_stopped():
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Node does not started...')

        n_type, operation, node_address, op_type = self._valiadate_packet(packet.parameters)
        is_force = packet.parameters.get('force', False)

        params = self.operator.process_manage_neighbours(n_type, operation, \
                                                node_address, op_type, is_force)

        if not params:
            return

        ret_params = packet.parameters
        ret_params.update(params)
        return FabnetPacketResponse(ret_parameters=ret_params)

    def after_process(self, packet, ret_packet):
        """In this method should be implemented logic that should be
        executed after response send

        @param packet - object of FabnetPacketRequest class
        @param ret_packet - object of FabnetPacketResponse class
        """
        if self.operator.is_stopped():
            return

        if ret_packet:
            if packet.parameters.get('operation', MNO_APPEND) == MNO_APPEND \
                    and ret_packet.ret_parameters.get('dont_append', False) == False:
                self.operator.rebalance_remove()

        self.operator.rebalance_append(packet.parameters)


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
        if self.operator.is_stopped():
            return

        if packet.ret_code != RC_OK:
            logger.warning('ManageNeighbour: neighbour does not process request...')
            self.operator.rebalance_remove()
            self.operator.rebalance_append({'neighbour_type': NT_SUPERIOR})
            self.operator.rebalance_append({'neighbour_type': NT_UPPER})
            return

        n_type, operation, node_address, op_type = self._valiadate_packet(packet.ret_parameters)
        dont_append = packet.ret_parameters.get('dont_append', False)
        dont_remove = packet.ret_parameters.get('dont_remove', False)

        self.operator.callback_manage_neighbours(n_type, operation, node_address, \
                                                    op_type, dont_append, dont_remove)

        self.operator.rebalance_remove()
        self.operator.rebalance_append(packet.ret_parameters)



