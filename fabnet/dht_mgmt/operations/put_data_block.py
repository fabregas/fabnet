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
from fabnet.core.constants import RC_OK, RC_ERROR

class PutDataBlockOperation(OperationBase):
    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.parameters.get('key', None)
        checksum = packet.parameters.get('checksum', None)
        is_replica = packet.parameters.get('is_replica', False)
        data = packet.binary_data

        if key is None or checksum is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='key or/and checksum does not found!')

        if hashlib.sha1(data).hexdigest() != checksum:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='data is corrupted!')

        dht_range = self.operator.get_dht_range()
        if not is_replica:
            dht_range.put(key, data)
        else:
            dht_range.put_replica(key, data)

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