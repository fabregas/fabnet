#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.get_data_block

@author Konstantin Andrusenko
@date October 3, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.dht_mgmt.fs_mapped_ranges import FSHashRangesNoData
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.dht_mgmt.constants import RC_NO_DATA
from fabnet.dht_mgmt.data_block import DataBlock
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

class GetDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.parameters.get('key', None)
        is_replica = packet.parameters.get('is_replica', False)
        if key is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Key is not found in request packet!')

        dht_range = self.operator.get_dht_range()
        try:
            if not is_replica:
                data = dht_range.get(key)
            else:
                data = dht_range.get_replica(key)
        except FSHashRangesNoData, err:
            return FabnetPacketResponse(ret_code=RC_NO_DATA, ret_message='No data found!')

        data_block = DataBlock(data)
        raw_data, raw_checksum = data_block.unpack()

        return FabnetPacketResponse(binary_data=raw_data, ret_parameters={'checksum': raw_checksum})


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
