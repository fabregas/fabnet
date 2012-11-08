#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.pull_subrange_request

@author Konstantin Andrusenko
@date November 08, 2012
"""
import os
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.dht_mgmt.constants import ALLOW_USED_SIZE_PERCENTS
from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE


class PullSubrangeRequestOperation(OperationBase):
    ROLES = [NODE_ROLE]

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        try:
            subrange_size = packet.parameters.get('subrange_size', None)
            if subrange_size is None:
                raise Exception('subrange_size does not found in PullSubrangeRequest packet')

            start_key = packet.parameters.get('start_key', None)
            if start_key is None:
                raise Exception('start_key does not found in PullSubrangeRequest packet')

            end_key = packet.parameters.get('end_key', None)
            if end_key is None:
                raise Exception('end_key does not found in PullSubrangeRequest packet')

            dht_range = self.operator.get_dht_range()
            if dht_range.get_subranges():
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Local range is spliited at this time...')

            self.__check_subrange_size(dht_range, subrange_size)
            self.__extend_range(packet.sender, dht_range, start_key, end_key)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='[PullSubrangeRequest] %s'%err)

        return FabnetPacketResponse()


    def __check_subrange_size(self, dht_range, subrange_size):
        subrange_size = int(subrange_size)
        estimated_data_size_perc = dht_range.get_estimated_data_percents(subrange_size)

        if estimated_data_size_perc >= ALLOW_USED_SIZE_PERCENTS:
            raise Exception('Subrange is so big for this node ;(')

    def __extend_range(self, sender, dht_range, start_key, end_key):
        old_range = self.operator.ranges_table.find(start_key)
        if old_range is None:
            raise Exception('No "parent" range found for subrange [%040x-%040x] in distributed ranges table'%(start_key, end_key))

        new_range = dht_range.extend(start_key, end_key)

        if old_range.start < start_key:
            new_foreign_range = (old_range.start, start_key-1, old_range.node_address)
        else:
            new_foreign_range = (end_key+1, old_range.end, old_range.node_address)

        old_foreign_range = (old_range.start, old_range.end, old_range.node_address)
        append_lst = [(new_range.get_start(), new_range.get_end(), self.operator.self_address)]
        append_lst.append(new_foreign_range)
        rm_lst = [(dht_range.get_start(), dht_range.get_end(), self.operator.self_address)]
        rm_lst.append(old_foreign_range)

        self.operator.update_dht_range(new_range)

        self._init_network_operation('UpdateHashRangeTable', {'append': append_lst, 'remove': rm_lst})


