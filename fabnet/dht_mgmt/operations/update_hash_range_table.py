#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.update_hash_range_table

@author Konstantin Andrusenko
@date September 26, 2012
"""
import time

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.dht_mgmt.constants import DS_NORMALWORK, MAX_HASH, MIN_HASH, WAIT_DHT_TABLE_UPDATE
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.dht_mgmt.hash_ranges_table import HashRange
from fabnet.utils.logger import logger

class UpdateHashRangeTableOperation(OperationBase):
    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        return packet

    def _check_near_range(self):
        if self.operator.status != DS_NORMALWORK:
            return

        self_dht_range = self.operator.get_dht_range()

        if self_dht_range.get_end() != MAX_HASH:
            next_range = self.operator.ranges_table.find(self_dht_range.get_end()+1)
            if not next_range:
                next_exists_range = self.operator.ranges_table.find_next(self_dht_range.get_end()-1)
                if next_exists_range:
                    end = next_exists_range.start-1
                else:
                    end = MAX_HASH
                new_dht_range = self_dht_range.extend(self_dht_range.get_end()+1, end)
                self.operator.update_dht_range(new_dht_range)

                rm_lst = [(self_dht_range.get_start(), self_dht_range.get_end(), self.operator.self_address)]
                append_lst = [(new_dht_range.get_start(), new_dht_range.get_end(), self.operator.self_address)]

                logger.info('Extended range by next neighbours')
                time.sleep(WAIT_DHT_TABLE_UPDATE)
                self._init_network_operation('UpdateHashRangeTable', {'append': append_lst, 'remove': rm_lst})

                return self._check_near_range()

        first_range = self.operator.ranges_table.find(MIN_HASH)
        if not first_range:
            first_range = self.operator.ranges_table.get_first()
            if first_range.node_address == self.operator.self_address:
                new_dht_range = self_dht_range.extend(MIN_HASH, first_range.start-1)
                self.operator.update_dht_range(new_dht_range)
                rm_lst = [(self_dht_range.get_start(), self_dht_range.get_end(), self.operator.self_address)]
                append_lst = [(new_dht_range.get_start(), new_dht_range.get_end(), self.operator.self_address)]
                logger.info('Extended range by first range')
                self._init_network_operation('UpdateHashRangeTable', {'append': append_lst, 'remove': rm_lst})


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        append_lst = packet.parameters.get('append', [])
        rm_lst = packet.parameters.get('remove', [])

        rm_obj_list = [HashRange(r[0], r[1], r[2]) for r in rm_lst]
        ap_obj_list = [HashRange(a[0], a[1], a[2]) for a in append_lst]
        self._lock()
        try:
            self.operator.ranges_table.validate_changes(rm_obj_list, ap_obj_list)

            for rm_range in rm_obj_list:
                range_obj = self.operator.ranges_table.find(rm_range.start)
                if range_obj:
                    self.operator.ranges_table.remove(rm_range.start)

            for app_range in ap_obj_list:
                self.operator.ranges_table.append(app_range.start, app_range.end, app_range.node_address)
        except Exception, err:
            logger.error('UpdateHashRangeTable error: %s'%err)
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err))
        finally:
            self._unlock()

        self.operator.check_dht_range()
        self._lock()
        try:
            self._check_near_range()
        finally:
            self._unlock()

        return FabnetPacketResponse()


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
        pass
