#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.fri_server
@author Konstantin Andrusenko
@date August 20, 2012

This module contains the implementation of FriServer class.
"""
import socket
import threading
import time
import traceback
from datetime import datetime
from Queue import Queue

import M2Crypto.SSL
from M2Crypto.SSL import Context, Connection

from fabnet.utils.logger import logger
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse,\
                                FriBinaryProcessor, FriException
from fabnet.core.socket_processor import SocketProcessor
from fabnet.core.constants import RC_OK, RC_ERROR, RC_REQ_CERTIFICATE, \
                STOP_THREAD_EVENT, S_ERROR, S_PENDING, S_INWORK, \
                BUF_SIZE, MIN_WORKERS_COUNT, MAX_WORKERS_COUNT

from fabnet.core.threads_manager import ThreadsManager
from fabnet.core.sessions_manager import SessionsManager

class FriServer:
    def __init__(self, hostname, port,  operator_obj, max_workers_count=20, server_name='fri-node',
                    keystorage=None):
        self.hostname = hostname
        self.port = port
        self.keystorage = keystorage

        self.queue = Queue()
        self.operator = operator_obj
        self.operator.set_node_name(server_name)
        self.operator.set_server(self)
        self.sessions = SessionsManager(self.operator.home_dir)
        self.stopped = True

        self.__workers_manager_thread = FriWorkersManager(self.queue, self.operator, \
                    keystorage, self.sessions, max_count=max_workers_count, workers_name=server_name)
        self.__workers_manager_thread.setName('%s-FriWorkersManager'%(server_name,))

        self.__conn_handler_thread = FriConnectionHandler(hostname, port, self.queue, keystorage)
        self.__conn_handler_thread.setName('%s-FriConnectionHandler'%(server_name,))

        cur_thread = threading.current_thread()
        cur_thread.setName('%s-MAIN'%server_name)

    def workers_stat(self):
        return self.__workers_manager_thread.get_workers_stat()

    def start(self):
        self.stopped = False

        self.__workers_manager_thread.start()
        self.__conn_handler_thread.start()

        while self.__conn_handler_thread.status == S_PENDING:
            time.sleep(.1)

        if self.__conn_handler_thread.status == S_ERROR:
            self.stop()
            logger.error('FriServer does not started!')
            return False
        else:
            logger.info('FriServer started!')
            return True

    def stop(self):
        if self.stopped:
            return

        self.operator.stop()
        self.__conn_handler_thread.stop()
        sock = None
        try:
            if self.keystorage:
                context = Context()
                context.set_verify(0, depth = 0)
                sock = Connection(context)
                sock.set_post_connection_check_callback(None)
                sock.set_socket_read_timeout(M2Crypto.SSL.timeout(sec=1))
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)

            if self.hostname == '0.0.0.0':
                hostname = '127.0.0.1'
            else:
                hostname = self.hostname

            sock.connect((hostname, self.port))
        except socket.error:
            pass
        except M2Crypto.SSL.SSLError:
            pass
        finally:
            if sock:
                sock.close()
                del sock

        self.__workers_manager_thread.stop()

        #waiting threads finishing... 
        self.__workers_manager_thread.join()
        self.__conn_handler_thread.join()
        self.stopped = True


class FriConnectionHandler(threading.Thread):
    def __init__(self, host, port, queue, keystorage):
        threading.Thread.__init__(self)
        self.queue = queue
        self.hostname = host
        self.port = port
        self.stopped = threading.Event()
        self.status = S_PENDING
        self.sock = None
        self.keystorage = keystorage

    def __bind_socket(self):
        try:
            if self.keystorage:
                context = self.keystorage.get_node_context()

                self.sock = Connection(context)
            else:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.bind((self.hostname, self.port))
            self.sock.listen(5)
        except Exception, err:
            self.status = S_ERROR
            logger.error('[__bind_socket] %s'%err)
        else:
            self.status = S_INWORK


    def run(self):
        logger.info('Starting connection handler thread...')
        self.__bind_socket()
        logger.info('Connection handler thread started!')

        while True:
            try:
                (sock, addr) = self.sock.accept()

                if self.stopped.is_set():
                    sock.close()
                    break

                self.queue.put(sock)
            except Exception, err:
                logger.error('[accept] %s'%err)

        if self.sock:
            self.sock.close()
            del self.sock
        logger.info('Connection handler thread stopped!')

    def stop(self):
        self.stopped.set()



class FriWorkersManager(ThreadsManager):
    def __init__(self, queue, operator, keystorage, sessions, min_count=MIN_WORKERS_COUNT, \
                    max_count=MAX_WORKERS_COUNT, workers_name='unnamed'):
        ThreadsManager.__init__(self, queue, min_count, max_count, workers_name, \
                                    FriWorker, (queue, operator, keystorage, sessions))




class FriWorker(threading.Thread):
    def __init__(self, queue, operator, keystorage, sessions):
        threading.Thread.__init__(self)

        self.queue = queue
        self.operator = operator
        self.sessions = sessions
        self.key_storage = keystorage
        self.__busy_flag = threading.Event()

    def is_busy(self):
        return self.__busy_flag.is_set()


    def check_session(self, sock_proc, session_id, send_allow=False):
        if not self.key_storage:
            if send_allow:
                sock_proc.send_packet(FabnetPacketResponse())
            return None

        if not session_id:
            raise Exception('SessionID does not found!')

        session = self.sessions.get(session_id)
        if session is None:
            cert_req_packet = FabnetPacketResponse(ret_code=RC_REQ_CERTIFICATE, ret_message='Certificate request')
            sock_proc.send_packet(cert_req_packet)
            cert_packet = sock_proc.get_packet()

            certificate = cert_packet['parameters'].get('certificate', None)

            if not certificate:
                raise Exception('No client certificate found!')

            role = self.key_storage.verify_cert(certificate)

            self.sessions.append(session_id, role)
            return role

        if send_allow:
            sock_proc.send_packet(FabnetPacketResponse())
        return session.role


    def run(self):
        logger.info('%s started!'%self.getName())
        ok_packet = FabnetPacketResponse(ret_code=RC_OK, ret_message='ok')
        ok_packet.dump()

        while True:
            ret_code = RC_OK
            ret_message = ''
            data = ''
            proc = None

            try:
                self.__busy_flag.clear()
                sock = self.queue.get()

                if sock == STOP_THREAD_EVENT:
                    logger.info('%s stopped!'%self.getName())
                    break

                self.__busy_flag.set()

                proc = SocketProcessor(sock)
                packet = proc.get_packet(True)
                session_id = packet.get('session_id', None)
                role = self.check_session(proc, session_id, packet.get('is_chunked', False))

                is_sync = packet.get('sync', False)
                if not is_sync:
                    proc.send_packet(ok_packet)
                    proc.close_socket()
                    proc = None


                if not packet.has_key('ret_code'):
                    pack = FabnetPacketRequest(**packet)

                    ret_packet = self.operator.process(pack, role)

                    try:
                        if not is_sync:
                            if ret_packet:
                                self.operator.send_to_sender(pack.sender, ret_packet)
                        else:
                            if not ret_packet:
                                ret_packet = FabnetPacketResponse()
                            proc.send_packet(ret_packet)
                            proc.close_socket(force=True)
                            proc = None
                    finally:
                        self.operator.after_process(pack, ret_packet)
                else:
                    self.operator.callback(FabnetPacketResponse(**packet))
            except Exception, err:
                ret_message = 'run() error: %s' % err
                logger.write = logger.debug
                traceback.print_exc(file=logger)
                logger.error(ret_message)
                try:
                    if proc:
                        err_packet = FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err))
                        proc.send_packet(err_packet)
                except Exception, err:
                    logger.error("Can't send error message to socket: %s"%err)

            finally:
                self.queue.task_done()
                if proc:
                    proc.close_socket(force=True)


