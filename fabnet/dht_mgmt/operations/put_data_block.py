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
from fabnet.core.constants import RC_OK, RC_ERROR, RC_PERMISSION_DENIED, \
                                    NODE_ROLE, CLIENT_ROLE
from fabnet.dht_mgmt.constants import RC_OLD_DATA, RC_NO_FREE_SPACE
from fabnet.dht_mgmt.data_block import DataBlockHeader
from fabnet.dht_mgmt.key_utils import KeyUtils
from fabnet.dht_mgmt.fs_mapped_ranges import TmpFile, FSHashRangesOldDataDetected, \
                    FSHashRangesNoFreeSpace, FSHashRangesPermissionDenied

class PutDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = "PutDataBlock"

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
        user_id = packet.parameters.get('user_id', None)
        carefully_save = packet.parameters.get('carefully_save', False)

        if not packet.binary_data:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Binary data does not found!')

        if key is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='key does not found!')

        key = KeyUtils.to_hex(key)
        data_list = []
        if primary_key:
            primary_key = KeyUtils.to_hex(primary_key)
            if replica_count is None:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='replica count for data block does not found!')
            if checksum is None:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='data checksum does not found!')
            if user_id is None:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='user_id does not found!')

            header = DataBlockHeader.pack(primary_key, replica_count, checksum, user_id)
            data_list.append(header)

        data_list.append(packet.binary_data)
        tempfile_path = self.operator.get_tempfile()
        try:
            tempfile = TmpFile(tempfile_path, data_list)
            self.operator.put_data_block(key, tempfile_path, is_replica, carefully_save)
        except FSHashRangesOldDataDetected, err:
            return FabnetPacketResponse(ret_code=RC_OLD_DATA, ret_message=str(err))
        except FSHashRangesNoFreeSpace, err:
            return FabnetPacketResponse(ret_code=RC_NO_FREE_SPACE, ret_message=str(err))
        except FSHashRangesPermissionDenied, err:
            return FabnetPacketResponse(ret_code=RC_PERMISSION_DENIED, ret_message=str(err))

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
