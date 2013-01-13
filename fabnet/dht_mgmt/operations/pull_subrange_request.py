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
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE


class PullSubrangeRequestOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'PullSubrangeRequest'

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

            self.operator.extend_range(subrange_size, start_key, end_key)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='[PullSubrangeRequest] %s'%err)

        return FabnetPacketResponse()




