#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator.async_call_agent
@author Konstantin Andrusenko
@date November 16, 2012
"""
import threading
import traceback
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.workers import ThreadBasedAbstractWorker
from fabnet.core.constants import RC_OK, RC_DONT_STARTED

class FriAgent(ThreadBasedAbstractWorker):
    def __init__(self, name, queue, operator):
        ThreadBasedAbstractWorker.__init__(self, name, queue)

        self.operator = operator
        self.self_address = operator.self_address
        self.fri_client = operator.fri_client

    def worker_routine(self, item):
        if len(item) != 2:
            raise Exception('Expected (<address>,<packet>), but "%s" occured'%item)

        address, packet = item

        rcode, rmsg = self.fri_client.call(address, packet)
        if rcode == RC_OK:
            return

        logger.error("Can't call async operation %s on %s. Details: %s"%\
            (getattr(packet, 'method', 'callback'), address, rmsg))
        logger.debug('Failed packet: %s'%packet)
        ret_packet = FabnetPacketResponse(message_id=packet.message_id, \
                       from_node=address, ret_code=RC_DONT_STARTED, ret_message=rmsg)

        if not self.operator.is_stopped():
            rcode, rmsg = self.fri_client.call(self.self_address, ret_packet)
            if rcode == RC_OK:
                return
            logger.error("Can't send error response to self node")


