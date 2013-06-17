#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.delete_data_block

@author Konstantin Andrusenko
@date June 17, 2013
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.dht_mgmt.fs_mapped_ranges import FSHashRangesPermissionDenied, FSHashRangesNoData 
from fabnet.core.constants import RC_OK, RC_ERROR, RC_PERMISSION_DENIED
from fabnet.dht_mgmt.constants import RC_NO_DATA
from fabnet.core.constants import NODE_ROLE

class DeleteDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'DeleteDataBlock'

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
            return FabnetPacketResponse(ret_code=RC_ERROR, \
                    ret_message='Key is not found in request packet!')
        carefully_delete = packet.parameters.get('carefully_delete', True)
        user_id = packet.parameters.get('user_id', None) 

        try:
            path = self.operator.delete_data_block(key, is_replica, user_id, carefully_delete)
        except FSHashRangesNoData, err:
            return FabnetPacketResponse(ret_code=RC_NO_DATA, \
                    ret_message='no data: %s'%err)
        except FSHashRangesPermissionDenied, err:
            return FabnetPacketResponse(ret_code=RC_PERMISSION_DENIED, \
                    ret_message='permission denied: %s'%err)

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
