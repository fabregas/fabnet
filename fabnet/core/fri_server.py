#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.fri_server
@author Konstantin Andrusenko
@date January 2, 2013

This module contains the implementation of FriServer class.
"""
import socket
import threading
import traceback
import time
from multiprocessing.queues import Queue
from multiprocessing.reduction import reduce_handle

from fabnet.utils.logger import core_logger as logger
from fabnet.core.constants import S_ERROR, S_PENDING, S_INWORK



class FriServer:
    def __init__(self, hostname, port, workers_manager, server_name='fri-node'):
        self.hostname = hostname
        self.port = port
        self.workers_manager = workers_manager

        self.stopped = True

        self.__conn_handler_thread = FriConnectionHandler(hostname, port, self.workers_manager.get_queue())
        self.__conn_handler_thread.setName('%s-FriConnectionHandler'%(server_name,))

    def start(self):
        self.stopped = False

        self.workers_manager.start_carefully()
        self.__conn_handler_thread.start()

        while self.__conn_handler_thread.status == S_PENDING:
            time.sleep(.1)

        if self.__conn_handler_thread.status == S_ERROR:
            self.stop()
            logger.error('FriServer does not started!')
            return False
        else:
            logger.info('FriServer is started!')
            return True

    def stop(self):
        if self.stopped:
            return

        logger.info('stopping FriServer...')
        self.__conn_handler_thread.stop()
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)

            if self.hostname == '0.0.0.0':
                hostname = '127.0.0.1'
            else:
                hostname = self.hostname

            sock.connect((hostname, self.port))
        except socket.error:
            pass
        finally:
            if sock:
                sock.close()
                del sock

        self.__conn_handler_thread.join()
        self.stopped = True
        self.workers_manager.stop()
        logger.info('FriServer is stopped!')


class FriConnectionHandler(threading.Thread):
    def __init__(self, host, port, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        if type(queue) == Queue:
            self.need_reduce = True
        else:
            self.need_reduce = False
        self.hostname = host
        self.port = port
        self.stopped = threading.Event()
        self.status = S_PENDING
        self.sock = None

    def __bind_socket(self):
        try:
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

                if self.need_reduce:
                    sock = reduce_handle(sock.fileno())

                self.queue.put(sock)
            except Exception, err:
                logger.write = logger.debug
                traceback.print_exc(file=logger)
                logger.error('[FriConnectionHandler.accept] %s'%err)

        if self.sock:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            del self.sock

        logger.info('Connection handler thread stopped!')

    def stop(self):
        self.stopped.set()


