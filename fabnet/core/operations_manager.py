#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operations_manager
@author Konstantin Andrusenko
@date January 5, 2013

This module contains the OperationsManager class implementation
"""
import traceback
from datetime import datetime
from multiprocessing import RLock

from fabnet.utils.internal import total_seconds
from fabnet.utils.logger import core_logger as logger
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.operator import OperatorClient
from fabnet.core.fri_client import FriClient
from fabnet.core.operation_base import OperationBase, PermissionDeniedException
from fabnet.core.constants import RC_ALREADY_PROCESSED, RC_PERMISSION_DENIED, \
                                RC_MESSAGE_ID_NOT_FOUND, KEEP_ALIVE_METHOD

class OperationsManager:
    def __init__(self, operations_classes, server_name, key_storage=None):
        self.__operations = {}
        self.__op_stat = None

        self.operator_cl = OperatorClient(server_name)
        self.__self_address = self.operator_cl.get_self_address()
        home_dir = self.operator_cl.get_home_dir()

        if key_storage:
            cert = key_storage.get_node_cert()
            ckey = key_storage.get_node_cert_key()
        else:
            cert = ckey = None

        self.__fri_client = FriClient(bool(cert), cert, ckey)

        for op_class in operations_classes:
            if not issubclass(op_class, OperationBase):
                raise Exception('Class %s does not inherit OperationBase class'%op_class)
            logger.debug('registering %s operation class...'% op_class.__name__)
            lock = RLock()
            operation = op_class(self.operator_cl, self.__fri_client, self.__self_address, home_dir, lock)
            self.__operations[op_class.get_name()] = operation

    def set_operation_stat(self, op_stat):
        self.__op_stat = op_stat

    def process(self, packet, role):
        """process request fabnet packet
        @param packet - object of FabnetPacketRequest class
        @param role - requestor role (None for disable auth)
        """
        t0 = None
        try:
            rcode = self.operator_cl.register_request(packet.message_id, packet.method, packet.sender)
            if rcode == RC_ALREADY_PROCESSED:
                return
            if packet.method == KEEP_ALIVE_METHOD:
                return FabnetPacketResponse(ret_code=rcode)

            if self.__op_stat is not None:
                t0 = datetime.now()

            operation_obj = self.__operations.get(packet.method, None)
            if operation_obj is None:
                if packet.is_multicast:
                    #transit packet
                    self.operator_cl.call_to_neighbours(packet.message_id, packet.method, packet.parameters, packet.is_multicast)
                    return
                else:
                    raise Exception('Method "%s" does not implemented! Available methods: %s'%(packet.method, self.__operations.keys()))

            operation_obj.check_role(role)

            logger.debug('processing %s'%packet)

            message_id = packet.message_id
            n_packet = operation_obj.before_resend(packet)
            if n_packet:
                self.operator_cl.call_to_neighbours(message_id, packet.method, packet.parameters, packet.is_multicast)

            s_packet = operation_obj.process(packet)
            if s_packet:
                s_packet.message_id = packet.message_id
                s_packet.from_node = self.__self_address

            return s_packet
        except PermissionDeniedException, err:
            return FabnetPacketResponse(from_node=self.__self_address,
                        message_id=packet.message_id, ret_code=RC_PERMISSION_DENIED,
                        ret_message='Permission denied!')
        except Exception, err:
            err_packet = FabnetPacketResponse(from_node=self.__self_address,
                            message_id=packet.message_id,
                            ret_code=1, ret_message= '[OpPROC] %s'%err)
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            logger.error('[Operator.process] %s'%err)
            return err_packet
        finally:
            if self.__op_stat is not None and t0:
                dt = total_seconds(datetime.now()-t0)
                self.__op_stat.update(packet.method, dt)


    def after_process(self, packet, ret_packet):
        """process some logic after response is send"""
        if packet.method == KEEP_ALIVE_METHOD:
            return

        operation_obj = self.__operations.get(packet.method, None)
        if operation_obj is None:
            return

        try:
            operation_obj.after_process(packet, ret_packet)
        except Exception, err:
            logger.error('%s after_process routine failed. Details: %s'%(packet.method, err))
            logger.write = logger.debug
            traceback.print_exc(file=logger)


    def callback(self, packet):
        """process callback fabnet packet
        @param packet - object of FabnetPacketResponse class
        """

        ret = self.operator_cl.register_callback(packet.message_id)
        if ret == RC_MESSAGE_ID_NOT_FOUND:
            raise Exception('MessageID does not found! Failed packet: %s'%packet)

        operation_name, sender = ret

        operation_obj = self.__operations.get(operation_name, None)
        if operation_obj is None:
            if sender:
                #transit packet
                self.send_to_sender(sender, packet)
            return

        s_packet = None
        try:
            s_packet = operation_obj.callback(packet, sender)
        except Exception, err:
            logger.error('%s callback failed. Details: %s'%(operation_name, err))
            logger.write = logger.debug
            traceback.print_exc(file=logger)

        if s_packet:
            self.send_to_sender(sender, packet)

    def send_to_sender(self, sender, packet):
        if sender is None:
            self.callback(packet)
            return

        self.operator_cl.response_to_sender(sender, packet.message_id, \
                        packet.ret_code, packet.ret_message, packet.ret_parameters)

    def get_session(self, session_id):
        return self.operator_cl.get_session(session_id)

    def put_session(self, session_id, role):
        return self.operator_cl.put_session(session_id, role)

