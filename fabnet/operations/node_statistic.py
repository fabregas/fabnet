#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.node_statistic

@author Konstantin Andrusenko
@date September 13, 2012
"""
import os
import resource
from datetime import datetime
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE, SI_SYS_INFO


class NodeStatisticOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'NodeStatistic'

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
        ret_params = {}
        operator_stat = self.operator.get_statistic()
        ret_params.update(operator_stat)

        reset_op_stat = packet.parameters.get('reset_op_stat', False)
        if reset_op_stat:
            self.operator.reset_statistic()

        loadavgstr = open('/proc/loadavg', 'r').readline().strip()
        data = loadavgstr.split()

        sysinfo = {}
        sysinfo['uptime'] = str(datetime.now() - self.operator.get_start_datetime())
        sysinfo['loadavg_5'] = data[0]
        sysinfo['loadavg_10'] = data[1]
        sysinfo['loadavg_15'] = data[2]

        ret_params[SI_SYS_INFO] = sysinfo
        return FabnetPacketResponse(ret_parameters=ret_params)

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
