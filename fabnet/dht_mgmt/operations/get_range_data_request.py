#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.get_data_range_request

@author Konstantin Andrusenko
@date September 25, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE
from fabnet.utils.logger import oper_logger as logger
import hashlib

class GetRangeDataRequestOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'GetRangeDataRequest'

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        try:
            self.operator.send_subrange_data(packet.sender)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Error: %s'%err)

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
        if packet.ret_code != RC_OK:
            logger.info('Trying select other hash range...')
            self.operator.start_as_dht_member()
        else:
            self.operator.set_status_to_normalwork(True) #with save_range=True
