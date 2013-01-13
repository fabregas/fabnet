#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operations_processor
@author Konstantin Andrusenko
@date January 5, 2013

This module contains the OperationsProcessor class implementation
"""
import traceback
import threading

from fabnet.utils.logger import oper_logger as logger
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, RC_INVALID_CERT, \
                                RC_REQ_CERTIFICATE, STAT_COLLECTOR_TIMEOUT, SO_OPERS_TIME
from fabnet.core.workers import ProcessBasedFriWorker
from fabnet.core.key_storage import InvalidCertificate
from fabnet.core.statistic import StatisticCollector, StatMap


class OperationsProcessor(ProcessBasedFriWorker):
    def __init__(self, name, queue, oper_manager, key_storage=None):
        ProcessBasedFriWorker.__init__(self, name, queue, key_storage)

        self.oper_manager = oper_manager
        self.ok_packet = FabnetPacketResponse(ret_code=RC_OK, ret_message='ok')

    def before_start(self):
        self.__op_stat = StatMap()
        cur_thread = threading.current_thread()
        thr_name = cur_thread.getName()
        self.oper_manager.set_operation_stat(self.__op_stat)

        self.__stat_collector = StatisticCollector(self.oper_manager.operator_cl, SO_OPERS_TIME, \
                                        thr_name, self.__op_stat, STAT_COLLECTOR_TIMEOUT)
        self.__stat_collector.start()

    def after_stop(self):
        self.__stat_collector.stop()

    def process(self, socket_processor):
        try:
            packet = socket_processor.recv_packet()

            if packet.is_request:
                is_chunked = packet.binary_chunk_cnt > 0
                role = self.check_session(socket_processor, packet.session_id, is_chunked)

            if not (packet.is_request and packet.sync):
                socket_processor.send_packet(self.ok_packet)
                socket_processor.close_socket()

            if packet.is_request:
                ret_packet = self.oper_manager.process(packet, role)

                try:
                    if not packet.sync:
                        if ret_packet:
                            self.oper_manager.send_to_sender(packet.sender, ret_packet)
                    else:
                        if not ret_packet:
                            ret_packet = FabnetPacketResponse()
                        socket_processor.send_packet(ret_packet)
                        socket_processor.close_socket(force=True)
                finally:
                    self.oper_manager.after_process(packet, ret_packet)
            else:
                self.oper_manager.callback(packet)
        except InvalidCertificate, err:
            if not socket_processor.is_closed():
                err_packet = FabnetPacketResponse(ret_code=RC_INVALID_CERT, ret_message=str(err))
                socket_processor.send_packet(err_packet)
        except Exception, err:
            ret_message = 'OperationsProcessor.process() error: %s' % err
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            logger.error(ret_message)
            try:
                if not socket_processor.is_closed():
                    err_packet = FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err))
                    socket_processor.send_packet(err_packet)
            except Exception, err:
                logger.error("Can't send error message to socket: %s"%err)
        finally:
            if socket_processor:
                socket_processor.close_socket(force=True)


    def check_session(self, sock_proc, session_id, send_allow=False):
        if not self._key_storage:
            if send_allow:
                sock_proc.send_packet(FabnetPacketResponse())
            return None

        if not session_id:
            raise Exception('SessionID does not found!')

        session = self.oper_manager.get_session(session_id)
        if session is None:
            cert_req_packet = FabnetPacketResponse(ret_code=RC_REQ_CERTIFICATE, ret_message='Certificate request')
            sock_proc.send_packet(cert_req_packet)
            cert_packet = sock_proc.recv_packet()

            certificate = cert_packet.parameters.get('certificate', None)

            if not certificate:
                raise InvalidCertificate('No client certificate found!')

            role = self._key_storage.verify_cert(certificate)

            self.oper_manager.put_session(session_id, role)
            return role

        if send_allow:
            sock_proc.send_packet(FabnetPacketResponse())
        return session.role

