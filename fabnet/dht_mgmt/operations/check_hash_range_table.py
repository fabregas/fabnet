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
import time

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_ERROR
from fabnet.dht_mgmt.constants import RC_NEED_UPDATE, DS_INITIALIZE, \
                                RANGES_TABLE_FLAPPING_TIMEOUT
from fabnet.utils.logger import logger

class CheckHashRangeTableOperation(OperationBase):
    def _get_ranges_table(self, from_addr, mod_index):
        if not self.operator.ranges_table.empty():
            for i in xrange(RANGES_TABLE_FLAPPING_TIMEOUT):
                time.sleep(1)
                c_mod_index = self.operator.ranges_table.get_mod_index()
                if c_mod_index == mod_index:
                    return

        logger.info('Ranges table is invalid! Requesting table from %s'% from_addr)
        self._init_operation(from_addr, 'GetRangesTable', {})

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        f_mod_index = packet.parameters.get('mod_index', None)
        if f_mod_index is None:
            raise Exception('Mod index parameter is expected for CheckHashRangeTable operation')

        c_mod_index = self.operator.ranges_table.get_mod_index()

        if c_mod_index == f_mod_index:
            return FabnetPacketResponse()

        logger.debug('f_mod_index=%s c_mod_index=%s'%(f_mod_index, c_mod_index))
        if f_mod_index > c_mod_index:
            #self._get_ranges_table(packet.sender, c_mod_index)
            return FabnetPacketResponse()
        else:
            return FabnetPacketResponse(ret_code=RC_NEED_UPDATE, ret_parameters={'mod_index': c_mod_index})


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
        if packet.ret_code == RC_ERROR:
            logger.error('CheckHashRangeTable failed on %s. Details: %s %s'%(packet.from_node, packet.ret_code, packet.ret_message))
        elif packet.ret_code == RC_NEED_UPDATE:
            self._get_ranges_table(packet.from_node, packet.ret_parameters['mod_index'])
