#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator
@author Konstantin Andrusenko
@date August 22, 2012

This module contains the Operator class implementation
"""
import copy
import threading
import traceback
import time
from datetime import datetime

from fabnet.core.operation_base import OperationBase
from fabnet.core.message_container import MessageContainer
from fabnet.core.constants import MC_SIZE
from fabnet.core.fri_base import FriClient, FabnetPacketRequest, FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, NT_SUPERIOR, NT_UPPER, \
                KEEP_ALIVE_METHOD, KEEP_ALIVE_TRY_COUNT, \
                KEEP_ALIVE_MAX_WAIT_TIME

class OperException(Exception):
    pass

class OperTimeoutException(OperException):
    pass


class Operator:
    def __init__(self, self_address, home_dir='/tmp/', certfile=None, is_init_node=False):
        self.__operations = {}
        self.msg_container = MessageContainer(MC_SIZE)

        self.__lock = threading.RLock()
        self.self_address = self_address
        self.home_dir = home_dir
        self.node_name = 'unknown-node'
        self.server = None

        self.superior_neighbours = []
        self.upper_neighbours = []
        self.fri_client = FriClient(certfile)

        self.__upper_keep_alives = {}
        self.__superior_keep_alives = {}

        self.start_datetime = datetime.now()
        self.is_init_node = is_init_node

    def stop(self):
        pass

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()

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
                logger.info('Superior neighbour %s appended'%address)
            elif neighbour_type == NT_UPPER:
                if address in self.upper_neighbours:
                    return
                self.upper_neighbours.append(address)
                logger.info('Upper neighbour %s appended'%address)
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
                logger.info('Superior neighbour %s removed'%address)
            elif neighbour_type == NT_UPPER:
                if address not in self.upper_neighbours:
                    return
                del self.upper_neighbours[self.upper_neighbours.index(address)]
                logger.info('Upper neighbour %s removed'%address)
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

    def on_neigbour_not_respond(self, neighbour_type, neighbour_address):
        """This method can be implemented in inherited class
        for some logic execution on not responsed neighbour"""
        pass

    def register_operation(self, op_name, op_class):
        if not issubclass(op_class, OperationBase):
            raise OperException('Class %s does not inherit OperationBase class'%op_class)

        self.__operations[op_name] = op_class(self)

    def call_node(self, node_address, packet, sync=False):
        self.msg_container.put(packet.message_id,
                        {'operation': packet.method,
                            'sender': None,
                            'responses_count': 0,
                            'datetime': datetime.now()})

        if sync:
            return self.fri_client.call_sync(node_address, packet.to_dict())
        else:
            return self.fri_client.call(node_address, packet.to_dict())


    def call_network(self, packet):
        packet.sender = None

        return self.fri_client.call(self.self_address, packet.to_dict())

    def _process_keep_alive(self, packet):
        if packet.sender not in self.get_neighbours(NT_UPPER):
            rcode = RC_ERROR
        else:
            rcode = RC_OK
            self.__lock.acquire()
            try:
                self.__upper_keep_alives[packet.sender] = datetime.now()
            finally:
                self.__lock.release()

        return FabnetPacketResponse(from_node=self.self_address,
                        message_id=packet.message_id, ret_code=rcode)

    def _rebalance_nodes(self):
        operation_obj = self.__operations.get('ManageNeighbour', None)
        if not operation_obj:
            logger.error('ManageNeighbour does not found. Cant rebalance node!')
            return

        operation_obj.rebalance_append({'neighbour_type': NT_SUPERIOR})
        operation_obj.rebalance_append({'neighbour_type': NT_UPPER})


    def check_neighbours(self):
        ka_packet = FabnetPacketRequest(method=KEEP_ALIVE_METHOD, sender=self.self_address, sync=True)
        superiors = self.get_neighbours(NT_SUPERIOR)

        remove_nodes = []
        for superior in superiors:
            resp = self.fri_client.call_sync(superior, ka_packet.to_dict())
            cnt = 0
            self.__lock.acquire()
            try:
                if self.__superior_keep_alives.get(superior, None) is None:
                    self.__superior_keep_alives[superior] = 0
                if resp.ret_code == RC_OK:
                    self.__superior_keep_alives[superior] = 0
                else:
                    self.__superior_keep_alives[superior] += 1


                cnt = self.__superior_keep_alives[superior]
            finally:
                self.__lock.release()

            if cnt == KEEP_ALIVE_TRY_COUNT:
                logger.info('Neighbour %s does not respond. removing it...'%superior)
                remove_nodes.append((NT_SUPERIOR, superior))
                self.__lock.acquire()
                del self.__superior_keep_alives[superior]
                self.__lock.release()


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
                    remove_nodes.append((NT_UPPER, upper))

                    del self.__upper_keep_alives[upper]
        finally:
            self.__lock.release()

        for n_type, nodeaddr in remove_nodes:
            self.remove_neighbour(n_type, nodeaddr)
            self.on_neigbour_not_respond(n_type, nodeaddr)

        if remove_nodes:
            self._rebalance_nodes()




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
                             'responses_count': 0,
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
                s_packet.from_node = self.self_address

            return s_packet
        except Exception, err:
            err_packet = FabnetPacketResponse(from_node=self.self_address,
                            message_id=packet.message_id,
                            ret_code=1, ret_message= '[Operator.process] %s'%err)
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            logger.error('[Operator.process] %s'%err)
            return err_packet

    def after_process(self, packet, ret_packet):
        """process some logic after response send"""
        if packet.method == KEEP_ALIVE_METHOD:
            return

        msg_info = self.msg_container.get(ret_packet.message_id)
        self.__lock.acquire()
        try:
            if not msg_info:
                raise OperException('Message with ID %s does not found!'%packet.message_id)

            operation = msg_info['operation']
        finally:
            self.__lock.release()

        operation_obj = self.__operations.get(operation, None)
        if operation_obj is None:
            raise OperException('Method "%s" does not implemented!'%operation)

        try:
            operation_obj.after_process(packet, ret_packet)
        except Exception, err:
            logger.error('%s after_process routine failed. Details: %s'%(operation, err))


    def callback(self, packet):
        """process callback fabnet packet
        @param packet - object of FabnetPacketResponse class
        """
        msg_info = self.msg_container.get(packet.message_id)
        self.__lock.acquire()
        try:
            if not msg_info:
                raise OperException('Message with ID %s does not found!'%packet.message_id)

            msg_info['responses_count'] += 1
            operation = msg_info['operation']
            sender = msg_info['sender']
        finally:
            self.__lock.release()

        operation_obj = self.__operations.get(operation, None)
        if operation_obj is None:
            raise OperException('Method "%s" does not implemented!'%operation)

        s_packet = None
        try:
            s_packet = operation_obj.callback(packet, sender)
        except Exception, err:
            logger.error('%s callback failed. Details: %s'%(operation, err))

        if s_packet:
            self.send_to_sender(sender, s_packet)

    def wait_response(self, message_id, timeout, response_count=1):
        for i in xrange(timeout*10):
            msg_info = self.msg_container.get(message_id)
            self.__lock.acquire()
            try:
                if msg_info['responses_count'] >= response_count:
                    break
            finally:
                self.__lock.release()

            time.sleep(.1)
        else:
            raise OperTimeoutException('Waiting %s response is timeouted'% message_id)


    def update_message(self, message_id, key, value):
        msg_info = self.msg_container.get(message_id)
        self.__lock.acquire()
        try:
            msg_info[key] = value
        finally:
            self.__lock.release()


    def get_message_item(self, message_id, key):
        msg_info = self.msg_container.get(message_id)
        self.__lock.acquire()
        try:
            item = msg_info.get(key, None)
            return copy.copy(item)
        finally:
            self.__lock.release()


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



