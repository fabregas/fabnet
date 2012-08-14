#!/usr/bin/python
"""
Copyright (C) 2011 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator_base
@author Konstantin Andrusenko
@date August 22, 2012

This module contains the Operator class implementation and
base OperationBase interface
"""
import uuid
from datetime import datetime

from fabnet.core.message_container import MessageContainer
from fabnet.core.constants import MC_SIZE
from fabnet.core.fri_base import FriClient, FabnetPacketRequest, FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK

class OperException(Exception):
    pass

class Operator:
    def __init__(self):
        self.__operations = {}
        self.msg_container = MessageContainer(MC_SIZE)

        self.neighbours = []
        self.fri_client = FriClient()

    def set_neighbour(self, neighbour):
        self.neighbours.append(neighbour)

    def remove_neighbour(self, neighbour):
        del self.neighbours[self.neighbours.index(neighbour)]

    def register_operation(self, op_name, op_class):
        self.__operations[op_name] = op_class(self)

    def call(self, node_address, packet):
        if not packet.message_id:
            packet.message_id = str(uuid.uuid1())

        self.msg_container.put(packet.message_id,
                        {'operation': packet.method,
                            'sender': None,
                            'datetime': datetime.now()})

        return self.fri_client.call(node_address, packet.to_dict())

    def process(self, packet):
        """process request fabnet packet
        @param packet - object of FabnetPacketRequest class
        """
        try:
            inserted = self.msg_container.put_safe(packet.message_id,
                            {'operation': packet.method,
                             'sender': packet.sender,
                             'datetime': datetime.now()})

            if not inserted:
                #this message is already processing/processed
                return

            operation_obj = self.__operations.get(packet.method, None)
            if operation_obj is None:
                raise OperException('Method "%s" does not implemented!'%packet.method)

            n_packet = operation_obj.transform_for_resend(packet)
            if n_packet:
                n_packet.message_id = packet.message_id
                self._send_to_neighbours(n_packet)

            s_packet = operation_obj.process(packet)
            if s_packet:
                s_packet.message_id = packet.message_id
                self._send_to_sender(packet.sender, s_packet)
        except Exception, err:
            err_packet = FabnetPacketResponse(message_id=packet.message_id,
                            ret_code=1, ret_message= '[Operator.process] %s'%err)
            self._send_to_sender(packet.sender, err_packet)
            logger.error('[Operator.process] %s'%err)


    def callback(self, packet):
        """process callback fabnet packet
        @param packet - object of FabnetPacketResponse class
        """
        msg_info = self.msg_container.get(packet.message_id)
        if not msg_info:
            raise OperException('Message with ID %s does not found!'%packet.message_id)

        operation_obj = self.__operations.get(msg_info['operation'], None)
        if operation_obj is None:
            raise OperException('Method "%s" does not implemented!'%msg_info['operation'])

        s_packet = operation_obj.callback(packet)

        if s_packet:
            self._send_to_sender(msg_info['sender'], s_packet)


    def _send_to_neighbours(self, packet):
        for neighbour in self.neighbours:
            ret = self.fri_client.call(neighbour, packet.to_dict())
            if ret != RC_OK:
                #node does not respond
                #TODO: implement node removing from network 
                logger.info('Neighbour %s does not respond!'%neighbour)


    def _send_to_sender(self, sender, packet):
        if sender is None:
            return

        rcode, rmsg = self.fri_client.call(sender, packet.to_dict())
        if rcode:
            logger.error('[Operator.send_back] %s %s'%(rcode, rmsg))





class OperationBase:
    def __init__(self, operator):
        self.operator = operator

    def transform_for_resend(self, packet):
        pass

    def process(self, packet):
        """call operation handler
        @param packet - dict with following keys:
                            - message_id (mandatory)
                            - method (mandatory)
                            - sender (optional)
                            - parameters (optional)
        @return req_packet, resp_packet
        """
        pass

    def callback(self, packet):
        pass

