#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator_base
@author Konstantin Andrusenko
@date August 22, 2012

This module contains the Operator class implementation and
base OperationBase interface
"""
import copy
import threading
import traceback
from datetime import datetime

from fabnet.core.message_container import MessageContainer
from fabnet.core.constants import MC_SIZE
from fabnet.core.fri_base import FriClient, FabnetPacketRequest, FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER, \
                KEEP_ALIVE_METHOD, KEEP_ALIVE_TRY_COUNT, \
                KEEP_ALIVE_MAX_WAIT_TIME

class OperException(Exception):
    pass


class Operator:
    def __init__(self, self_address, home_dir='/tmp/'):
        self.__operations = {}
        self.msg_container = MessageContainer(MC_SIZE)

        self.__lock = threading.RLock()
        self.self_address = self_address
        self.home_dir = home_dir
        self.node_name = 'unknown-node'
        self.server = None

        self.superior_neighbours = []
        self.upper_neighbours = []
        self.fri_client = FriClient()

        self.__upper_keep_alives = {}
        self.__superior_keep_alives = {}

        self.start_datetime = datetime.now()

    def set_node_name(self, node_name):
        self.node_name = node_name

    def set_server(self, server):
        self.server = server

    def set_neighbour(self, neighbour_type, address):
        self.__lock.acquire()
        try:
            if neighbour_type == NT_SUPERIOR:
                if address in self.superior_neighbours:
                    return
                self.superior_neighbours.append(address)
            elif neighbour_type == NT_UPPER:
                if address in self.upper_neighbours:
                    return
                self.upper_neighbours.append(address)
            else:
                raise OperException('Neigbour type "%s" is invalid!'%neighbour_type)
        finally:
            self.__lock.release()


    def remove_neighbour(self, neighbour_type, address):
        self.__lock.acquire()
        try:
            if neighbour_type == NT_SUPERIOR:
                if address not in self.superior_neighbours:
                    return
                del self.superior_neighbours[self.superior_neighbours.index(address)]
            elif neighbour_type == NT_UPPER:
                if address not in self.upper_neighbours:
                    return
                del self.upper_neighbours[self.upper_neighbours.index(address)]
            else:
                raise OperException('Neigbour type "%s" is invalid!'%neighbour_type)
        finally:
            self.__lock.release()

    def get_neighbours(self, neighbour_type):
        self.__lock.acquire()
        try:
            if neighbour_type == NT_SUPERIOR:
                return copy.copy(self.superior_neighbours)
            elif neighbour_type == NT_UPPER:
                return copy.copy(self.upper_neighbours)
            else:
                raise OperException('Neigbour type "%s" is invalid!'%neighbour_type)
        finally:
            self.__lock.release()

    def register_operation(self, op_name, op_class):
        self.__operations[op_name] = op_class(self)

    def call_node(self, node_address, packet):
        self.msg_container.put(packet.message_id,
                        {'operation': packet.method,
                            'sender': None,
                            'datetime': datetime.now()})

        return self.fri_client.call(node_address, packet.to_dict())


    def call_network(self, packet):
        packet.sender = None

        return self.fri_client.call(self.self_address, packet.to_dict())

    def _process_keep_alive(self, packet):
        self.__lock.acquire()
        try:
            self.__upper_keep_alives[packet.sender] = datetime.now()
        finally:
            self.__lock.release()

    def check_neighbours(self):
        ka_packet = FabnetPacketRequest(method=KEEP_ALIVE_METHOD, sender=self.self_address)
        superiors = self.get_neighbours(NT_SUPERIOR)

        for superior in superiors:
            code, msg = self.fri_client.call(superior, ka_packet.to_dict())
            cnt = 0
            self.__lock.acquire()
            try:
                if self.__superior_keep_alives.get(superior, None) is None:
                    self.__superior_keep_alives[superior] = 0
                if code:
                    self.__superior_keep_alives[superior] += 1
                else:
                    self.__superior_keep_alives[superior] = 0

                cnt = self.__superior_keep_alives[superior]
            finally:
                self.__lock.release()

            if cnt == KEEP_ALIVE_TRY_COUNT:
                logger.info('Neighbour %s does not respond. removing it...'%superior)
                self.remove_neighbour(NT_SUPERIOR, superior)

        #check upper nodes...
        uppers = self.get_neighbours(NT_UPPER)
        self.__lock.acquire()
        try:
            cur_dt = datetime.now()
            for upper in uppers:
                ka_dt = self.__upper_keep_alives.get(upper, None)
                if ka_dt == None:
                    self.__upper_keep_alives[upper] = datetime.now()
                    continue
                delta = cur_dt - ka_dt
                if delta.total_seconds() >= KEEP_ALIVE_MAX_WAIT_TIME:
                    logger.info('No keep alive packets from upper neighbour %s. removing it...'%upper)
                    self.remove_neighbour(NT_UPPER, upper)
        finally:
            self.__lock.release()



    def process(self, packet):
        """process request fabnet packet
        @param packet - object of FabnetPacketRequest class
        """
        try:
            if packet.method == KEEP_ALIVE_METHOD:
                return self._process_keep_alive(packet)

            inserted = self.msg_container.put_safe(packet.message_id,
                            {'operation': packet.method,
                             'sender': packet.sender,
                             'datetime': datetime.now()})

            if not inserted:
                #this message is already processing/processed
                logger.debug('packet is already processing/processed: %s'%packet)
                return

            operation_obj = self.__operations.get(packet.method, None)
            if operation_obj is None:
                raise OperException('Method "%s" does not implemented!'%packet.method)

            logger.debug('processing packet %s'%packet)

            n_packet = packet.copy()
            n_packet = operation_obj.before_resend(n_packet)
            if n_packet:
                n_packet.message_id = packet.message_id
                n_packet.sync = False
                self._send_to_neighbours(n_packet)

            s_packet = operation_obj.process(packet)
            if s_packet:
                s_packet.message_id = packet.message_id

            return s_packet
        except Exception, err:
            err_packet = FabnetPacketResponse(message_id=packet.message_id,
                            ret_code=1, ret_message= '[Operator.process] %s'%err)
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            logger.error('[Operator.process] %s'%err)
            return err_packet


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

        s_packet = operation_obj.callback(packet, msg_info['sender'])

        if s_packet:
            self.send_to_sender(msg_info['sender'], s_packet)


    def _send_to_neighbours(self, packet):
        packet.sender = self.self_address
        neighbours = self.get_neighbours(NT_SUPERIOR)

        for neighbour in neighbours:
            ret, message = self.fri_client.call(neighbour, packet.to_dict())
            if ret != RC_OK:
                logger.info('Neighbour %s does not respond! Details: %s'%(neighbour, message))


    def send_to_sender(self, sender, packet):
        if sender is None:
            self.callback(packet)
            return

        rcode, rmsg = self.fri_client.call(sender, packet.to_dict())
        if rcode:
            logger.error('[Operator.send_back] %s %s'%(rcode, rmsg))





class OperationBase:
    def __init__(self, operator):
        self.operator = operator
        self.__lock = threading.Lock()

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

    def _init_operation(self, node_address, operation, parameters):
        """Initiate new operation"""
        req = FabnetPacketRequest(method=operation, sender=self.operator.self_address, parameters=parameters)
        self.operator.call_node(node_address, req)

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()


