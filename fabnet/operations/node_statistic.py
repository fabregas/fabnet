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
from fabnet.core.constants import NT_SUPERIOR, NT_UPPER, \
                        ONE_DIRECT_NEIGHBOURS_COUNT
from fabnet.operations.constants import NB_NORMAL, NB_MORE, NB_LESS
from fabnet.core.fri_base import FabnetPacketResponse


class NodeStatisticOperation(OperationBase):
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
        uppers = self.operator.get_neighbours(NT_UPPER)
        superiors = self.operator.get_neighbours(NT_SUPERIOR)
        ret_params = {}

        if len(uppers) == ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['uppers_balance'] = NB_NORMAL
        elif len(uppers) > ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['uppers_balance'] = NB_MORE
        else:
            ret_params['uppers_balance'] = NB_LESS

        if len(superiors) == ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['superiors_balance'] = NB_NORMAL
        elif len(superiors) > ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['superiors_balance'] = NB_MORE
        else:
            ret_params['superiors_balance'] = NB_LESS

        count, busy = self.operator.server.workers_stat()
        ret_params['workers_count'] = count
        ret_params['workers_busy'] = busy

        loadavgstr = open('/proc/loadavg', 'r').readline().strip()
        data = loadavgstr.split()

        rss = threads = ''
        lines = open('/proc/%i/status'%os.getpid(),'r').readlines()
        for line in lines:
            (param, value) = line.split()[:2]
            if param.startswith('VmRSS'):
                rss = value.strip()
            elif param.startswith('Threads'):
                threads = value.strip()
        res = resource.getrusage(resource.RUSAGE_SELF)

        ret_params['loadavg_5'] = data[0]
        ret_params['loadavg_10'] = data[1]
        ret_params['loadavg_15'] = data[2]
        ret_params['utime'] = str(res.ru_utime)
        ret_params['stime'] = str(res.ru_stime)
        ret_params['memory'] = rss
        ret_params['threads'] = threads
        ret_params['uptime'] = str(datetime.now() - self.operator.start_datetime)
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
