#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.split_range_request

@author Konstantin Andrusenko
@date September 23, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.dht_mgmt.constants import ALLOW_FREE_SIZE_PERCENTS
from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE


class SplitRangeRequestOperation(OperationBase):
    ROLES = [NODE_ROLE]
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


        dht_range = self.operator.get_dht_range()

        subranges = dht_range.get_subranges()
        if subranges:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Already splitting')

        ret_range, new_range = dht_range.split_range(start_key, end_key)

        range_size = ret_range.get_range_size()

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
            logger.info('Trying select other hash range...')
            self.operator.start_as_dht_member()
        else:
            free_size = self.operator.get_dht_range().get_free_size()
            if (int(packet.ret_parameters['range_size']) * 100. / free_size) > ALLOW_FREE_SIZE_PERCENTS:
                logger.info('Requested range is huge for me :( canceling...')
                self._init_operation(packet.from_node, 'SplitRangeCancel', {})
            else:
                logger.info('Requesting new range data from %s...'%packet.from_node)
                self._init_operation(packet.from_node, 'GetRangeDataRequest', {})


