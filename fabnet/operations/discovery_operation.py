#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.discovery_operation

@author Konstantin Andrusenko
@date September 7, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import NT_SUPERIOR, NT_UPPER, \
                        NODE_ROLE, CLIENT_ROLE, RC_OK
from fabnet.utils.logger import logger

class DiscoveryOperation(OperationBase):
    ROLES = [NODE_ROLE]

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        uppers = self.operator.get_neighbours(NT_UPPER)
        superiors = self.operator.get_neighbours(NT_SUPERIOR)
        return FabnetPacketResponse(ret_parameters={'uppers': uppers, \
                'superiors': superiors, 'node': self.self_address})

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
        if packet.ret_code != RC_OK:
            logger.error('No discovery response from neighbour.. It makes me sad panda :(')
            return

        node = packet.ret_parameters['node']
        uppers = packet.ret_parameters.get('uppers', [])
        superiors = packet.ret_parameters.get('superiors', [])

        self.operator.start_discovery_process(node, uppers, superiors)

