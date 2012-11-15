#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.get_node_config

@author Konstantin Andrusenko
@date November 15, 2012
"""
import os
import resource
from datetime import datetime
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import NODE_ROLE, RC_ERROR
from fabnet.core.config import Config

class GetNodeConfigOperation(OperationBase):
    ROLES = [NODE_ROLE]

    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        pass

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        return FabnetPacketResponse(ret_parameters=Config.get_config_dict())

