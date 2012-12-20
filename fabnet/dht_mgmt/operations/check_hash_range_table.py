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
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE, RC_DONT_STARTED
from fabnet.dht_mgmt.constants import RC_NEED_UPDATE, DS_INITIALIZE
from fabnet.core.config import Config
from fabnet.utils.logger import logger

class CheckHashRangeTableOperation(OperationBase):
    ROLES = [NODE_ROLE]

    def _get_ranges_table(self, from_addr, mod_index, ranges_count):
        if not self.operator.ranges_table.empty():
            for i in xrange(Config.RANGES_TABLE_FLAPPING_TIMEOUT):
                time.sleep(1)
                c_mod_index = self.operator.ranges_table.get_mod_index()
                c_ranges_count = self.operator.ranges_table.count()
                if c_mod_index == mod_index and ranges_count == c_ranges_count:
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
        if self.operator.status == DS_INITIALIZE:
            return FabnetPacketResponse(ret_code=RC_DONT_STARTED, ret_message='Node is not initialized yet!')

        f_mod_index = packet.parameters.get('mod_index', None)
        if f_mod_index is None:
            raise Exception('Mod index parameter is expected for CheckHashRangeTable operation')

        ranges_count = packet.parameters.get('ranges_count', None)
        if ranges_count is None:
            raise Exception('ranges_count parameter is expected for CheckHashRangeTable operation')

        range_start = packet.parameters.get('range_start', None)
        if range_start is None:
            raise Exception('range_start parameter is expected for CheckHashRangeTable operation')

        range_end = packet.parameters.get('range_end', None)
        if range_end is None:
            raise Exception('range_end parameter is expected for CheckHashRangeTable operation')

        c_mod_index = self.operator.ranges_table.get_mod_index()

        if c_mod_index == f_mod_index:
            return FabnetPacketResponse()

        found_range = self._find_range(range_start, range_end, packet.sender)
        c_ranges_count = self.operator.ranges_table.count()
        if not found_range:
            logger.debug('CheckHashRangeTable: sender range does not found in local hash table...')
            if ranges_count < c_ranges_count:
                return FabnetPacketResponse(ret_code=RC_NEED_UPDATE, \
                        ret_parameters={'mod_index': c_mod_index, 'ranges_count': c_ranges_count})
            else:
                return FabnetPacketResponse()

        logger.debug('CheckHashRangeTable: f_mod_index=%s c_mod_index=%s'%(f_mod_index, c_mod_index))
        if f_mod_index > c_mod_index:
            #self._get_ranges_table(packet.sender, c_mod_index)
            return FabnetPacketResponse()
        else:
            return FabnetPacketResponse(ret_code=RC_NEED_UPDATE, \
                    ret_parameters={'mod_index': c_mod_index, 'ranges_count': c_ranges_count})

    def _find_range(self, range_start, range_end, sender):
        h_range = self.operator.ranges_table.find(range_start)
        if not h_range:
            return False

        if h_range.start == range_start  and h_range.end == range_end \
            and h_range.node_address:
            return True

        return False


    def _remove_node_range(self, nodeaddr):
        for range_obj in self.operator.ranges_table.iter_table():
            if range_obj.node_address == nodeaddr:
                logger.info('Node %s went from DHT. Updating hash range table on network...'%range_obj.node_address)
                rm_lst = [(range_obj.start, range_obj.end, range_obj.node_address)]
                parameters = {'append': [], 'remove': rm_lst}
                self._init_network_operation('UpdateHashRangeTable', parameters)
                break


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
        if packet.ret_code == RC_DONT_STARTED:
            self._remove_node_range(packet.from_node)
            time.sleep(Config.WAIT_DHT_TABLE_UPDATE)
            self.operator.check_near_range()
        elif packet.ret_code == RC_OK:
            self.operator.check_near_range()
        elif packet.ret_code == RC_ERROR:
            logger.error('CheckHashRangeTable failed on %s. Details: %s %s'%(packet.from_node, \
                    packet.ret_code, packet.ret_message))
        elif packet.ret_code == RC_NEED_UPDATE:
            self._get_ranges_table(packet.from_node, packet.ret_parameters['mod_index'], \
                    packet.ret_parameters['ranges_count'])
