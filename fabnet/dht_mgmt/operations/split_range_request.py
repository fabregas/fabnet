#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.split_range_request

@author Konstantin Andrusenko
@date September 23, 2012
"""
import os
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE


class SplitRangeRequestOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'SplitRangeRequest'

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        start_key = packet.parameters.get('start_key', None)
        if start_key is None:
            raise Exception('start_key is not found in SplitRangeRequest packet')

        end_key = packet.parameters.get('end_key', None)
        if end_key is None:
            raise Exception('end_key is not found in SplitRangeRequest packet')

        try:
            range_size = self.operator.split_range(start_key, end_key)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err))

        logger.debug('Range is splitted for %s. Subrange size: %s'%(packet.sender, range_size))

        return FabnetPacketResponse(ret_parameters={'range_size': range_size})


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
            logger.error('Cant split range from %s. Details: %s'%(sender, packet.ret_message))
            logger.info('Trying select other hash range...')
            self.operator.start_as_dht_member()
        else:
            subrange_size = int(packet.ret_parameters['range_size'])
            self.operator.accept_foreign_subrange(packet.from_node, subrange_size)


