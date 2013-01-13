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
from fabnet.core.fri_base import FabnetPacketRequest

class PermissionDeniedException(Exception):
    pass

class OperationBase:
    ROLES = []
    NAME = None

    @classmethod
    def get_name(cls):
        if cls.NAME is None:
            cls.NAME = cls.__name__
        return cls.NAME

    @classmethod
    def check_role(cls, role):
        if role is None:
            #no security mode enabled
            return

        if role not in cls.ROLES:
            raise PermissionDeniedException('Permission denied!')

    def __init__(self, operator, fri_client, self_address, home_dir, lock_obj):
        self.operator = operator
        self.self_address = self_address
        self.home_dir = home_dir
        self.__fri_client = fri_client
        self.__lock = lock_obj
        self.init_locals()

    def init_locals(self):
        """This method should be implemented for some local
        variables initialization
        """
        pass

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
        if sync:
            req = FabnetPacketRequest(method=operation, sender=self.self_address, \
                    parameters=parameters, binary_data=binary_data, sync=sync)
            return self.__fri_client.call_sync(node_address, req)

        if binary_data:
            raise Exception('Sending binary data from operation in async mode is disabled!')
        message_id = self.operator.async_remote_call(node_address, operation, parameters, False)
        return message_id

    def _init_network_operation(self, operation, parameters):
        """Initiate new operation over fabnet network"""
        message_id = self.operator.async_remote_call(None, operation, parameters, True)

    def _throw_event(self, event_type, event_topic, event_message):
        self._init_network_operation('NotifyOperation', {'event_type':str(event_type), \
                'event_message':str(event_message), 'event_topic': str(event_topic), \
                'event_provider':self.self_address})

    def _cache_response(self, packet):
        """Cache response from node for using it from other operations objects"""
        self.operator.update_message(packet.message_id, packet.from_node, packet.ret_parameters)

    def _get_cached_response(self, message_id, from_node):
        """Get cached response from some node"""
        return self.operator.get_message_item(message_id, from_node)

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()
