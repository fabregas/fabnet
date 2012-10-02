#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.check_hash_range_table

@author Konstantin Andrusenko
@date October 3, 2012
"""
from datetime import datetime

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.dht_mgmt.constants import RC_NEED_UPDATE
from fabnet.utils.logger import logger

class CheckHashRangeTableOperation(OperationBase):
    def _get_ranges_table(self, from_addr):
        logger.info('Ranges table is invalid! Requesting table from %s'% from_addr)
        self._init_operation(from_addr, 'GetRangesTable', {})

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        f_checksum = packet.parameters.get('checksum', None)
        f_last_dm = packet.parameters.get('last_dm', None)
        if f_last_dm is None or f_checksum is None:
            raise Exception('Checksum and Last DM expected for CheckHashRangeTable operation')
        f_last_dm = datetime.strptime(f_last_dm, '%Y-%m-%dT%H:%M:%S.%f')

        c_checksum, c_last_dm = self.operator.ranges_table.get_checksum()
        if c_checksum == f_checksum:
            return FabnetPacketResponse()

        logger.info('f_last_dm=%s c_last_dm=%s'%(f_last_dm, c_last_dm))
        if f_last_dm > c_last_dm:
            self._get_ranges_table(packet.sender)
            return FabnetPacketResponse()
        else:
            return FabnetPacketResponse(ret_code=RC_NEED_UPDATE)


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
        if packet.ret_code == RC_NEED_UPDATE:
            self._get_ranges_table(packet.from_node)
