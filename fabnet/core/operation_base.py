#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operation_base
@author Konstantin Andrusenko
@date September 18, 2012

This module contains the OperationBase interface
"""

import threading
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.utils.logger import logger

class OperationBase:
    ROLES = []

    def __init__(self, operator):
        self.operator = operator
        self.__lock = threading.RLock()

    @classmethod
    def check_role(cls, role):
        if role is None:
            #no security mode enabled
            return

        if role not in cls.ROLES:
            raise Exception('Permission denied!')

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
        pass

    def after_process(self, packet, ret_packet):
        """In this method should be implemented logic that should be
        executed after response send
        If ret_packet is None this method is not called

        @param packet - object of FabnetPacketRequest class
        @param ret_packet - object of FabnetPacketResponse class
        """
        pass

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

    def _init_operation(self, node_address, operation, parameters, sync=False, binary_data=''):
        """Initiate new operation"""
        req = FabnetPacketRequest(method=operation, sender=self.operator.self_address, \
                parameters=parameters, binary_data=binary_data, sync=sync)
        resp = self.operator.call_node(node_address, req, sync)
        if sync:
            return resp
        else:
            rcode, rmsg = resp
            if rcode:
                logger.error('Operation %s failed on %s. Details: %s'%(operation, node_address, rmsg))
            return rcode, rmsg

    def _init_network_operation(self, operation, parameters):
        """Initiate new operation over fabnet network"""
        req = FabnetPacketRequest(method=operation, sender=self.operator.self_address, parameters=parameters)
        self.operator.call_network(req)

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()

    def _cache_response(self, packet):
        """Cache response from node for using it from other operations objects"""
        self.operator.update_message(packet.message_id, packet.from_node, packet.ret_parameters)

    def _get_cached_response(self, message_id, from_node):
        """Get cached response from some node"""
        return self.operator.get_message_item(message_id,from_node)

    def _throw_event(self, event_type, event_message):
        self._init_network_operation('NotifyOperation', {'event_type':event_type, \
                'event_message':event_message, 'event_provider':self.operator.self_address})

