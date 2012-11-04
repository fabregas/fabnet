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
from datetime import datetime

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.utils.logger import logger


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
        if self.operator.has_type('Monitoring'):
            self.__save_event(packet)

    def __save_event(packet):
        try:
            event_type = packet.parameters.get('event_type', None)
            event_provider = packet.parameters.get('event_provider', None)
            if event_provider is None:
                raise Exception('event_provider does not found!')

            event_message = packet.parameters.get('event_message', None)

            conn = sqlite3.connect(os.path.join(self.operator.home_dir, TOPOLOGY_DB)) #TODO: make me persistent
            curs = conn.cursor()
            curs.execute("INSERT INTO fabnet_event (event_type, event_provider, event_message) VALUES (%s, %s, %s)",
                            (event_type, event_provider, event_message))
            conn.commit()

            curs.close()
            conn.close()
        except Exception, err:
            logger.error('[NotifyOperation] %s'%err)

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
