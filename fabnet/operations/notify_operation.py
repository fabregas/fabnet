#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.notify_operation

@author Konstantin Andrusenko
@date Novenber 5, 2012
"""
import os

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import NODE_ROLE, ET_INFO, ET_ALERT


class NotifyOperation(OperationBase):
    ROLES = [NODE_ROLE]

    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        return packet

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        try:
            event_type = packet.parameters.get('event_type', None)
            event_provider = packet.parameters.get('event_provider', None)
            event_topic = packet.parameters.get('event_topic', None)
            if event_provider is None:
                raise Exception('event_provider does not found!')

            event_message = packet.parameters.get('event_message', None)

            if packet.sender is None: #this is sender
                if event_type == ET_ALERT:
                    logger.warning('[ALERT][%s] *%s* %s'%(event_provider, event_topic, event_message))
                elif event_type == ET_INFO:
                    logger.info('[INFORMATION][%s] *%s* %s'%(event_provider, event_topic,  event_message))
                else:
                    logger.info('[NOTIFICATION.%s][%s] *%s* %s'%(event_type, event_provider, event_topic, event_message))

            self.on_network_notify(event_type, event_provider, event_topic, event_message)
        except Exception, err:
            logger.error('[NotifyOperation] %s'%err)

    def on_network_notify(self, event_type, event_provider, event_topic, event_message):
        """This method can be implemented for processing network notification"""
        pass

    def callback(self, packet, sender):
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
