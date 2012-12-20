#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.put_data_block

@author Konstantin Andrusenko
@date September 26, 2012
"""
import hashlib

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, \
                                    NODE_ROLE, CLIENT_ROLE
from fabnet.dht_mgmt.constants import RC_OLD_DATA, RC_NO_FREE_SPACE
from fabnet.dht_mgmt.data_block import DataBlock
from fabnet.dht_mgmt.fs_mapped_ranges import FSHashRangesOldDataDetected, FSHashRangesNoFreeSpace

class PutDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        primary_key = packet.parameters.get('primary_key', None)
        replica_count =  packet.parameters.get('replica_count', None)
        key = packet.parameters.get('key', None)
        checksum = packet.parameters.get('checksum', None)
        is_replica = packet.parameters.get('is_replica', False)
        carefully_save = packet.parameters.get('carefully_save', False)
        data = packet.binary_data

        if key is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='key does not found!')
        if checksum is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='data checksum does not found!')

        if primary_key:
            if replica_count is None:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='replica_count does not found!')

            data_block = DataBlock(data, checksum)
            data_block.validate()
            data, checksum = data_block.pack(primary_key, replica_count)
        else:
            #DataBlock.read_header(data) #FIXME
            pass

        dht_range = self.operator.get_dht_range()
        try:
            if not is_replica:
                dht_range.put(key, data, carefully_save)
            else:
                dht_range.put_replica(key, data, carefully_save)
        except FSHashRangesOldDataDetected, err:
            return FabnetPacketResponse(ret_code=RC_OLD_DATA, ret_message=str(err))
        except FSHashRangesNoFreeSpace, err:
            return FabnetPacketResponse(ret_code=RC_NO_FREE_SPACE, ret_message=str(err))

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
