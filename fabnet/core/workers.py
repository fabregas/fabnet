#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.workers
@author Konstantin Andrusenko
@date January 03, 2013
"""
import threading
import traceback
import socket
import multiprocessing as mp

from fabnet.utils.logger import logger
from fabnet.core.constants import STOP_WORKER_EVENT
from fabnet.core.socket_processor import SocketProcessor
from multiprocessing.reduction import rebuild_handle

from M2Crypto.SSL import Context, Connection

class ThreadBasedAbstractWorker(threading.Thread):
    is_threaded = True

    def __init__(self, name, queue):
        threading.Thread.__init__(self)

        self.setName(name)
        self.__queue = queue
        self.__busy_flag = threading.Event()

    def is_busy(self):
        return self.__busy_flag.is_set()

    def run(self):
        self.before_start()
        logger.info('worker is started!')
        while True:
            data = self.__queue.get()
            if data == STOP_WORKER_EVENT:
                break

            self.__busy_flag.set()
            try:
                self.worker_routine(data)
            except Exception, err:
                logger.error(err)
                logger.write = logger.debug
                traceback.print_exc(file=logger)
            finally:
                #self.__queue.task_done()
                self.__busy_flag.clear()

        self.after_stop()
        logger.info('worker is stopped!')

    def worker_routine(self, data):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')

    def before_start(self):
        """This method can be implemented in inherited class"""
        pass

    def after_stop(self):
        """This method can be implemented in inherited class"""
        pass


class ProcessBasedAbstractWorker(mp.Process):
    is_threaded = False

    def __init__(self, name, queue):
        mp.Process.__init__(self)
        self.__is_busy = mp.Value("b", False, lock=mp.Lock())
        self.__queue = queue
        self.__name = name

    def getName(self):
        return self.__name

    def is_busy(self):
        return self.__is_busy.value

    def run(self):
        cur_thread = threading.current_thread()
        cur_thread.setName(self.__name)

        self.before_start()

        logger.info('worker is started!')
        while True:
            data = self.__queue.get()
            if data == STOP_WORKER_EVENT:
                break

            self.__is_busy.value = True
            try:
                self.worker_routine(data)
            except Exception, err:
                logger.error(err)
                logger.write = logger.debug
                traceback.print_exc(file=logger)
            finally:
                #self.__queue.task_done()
                self.__is_busy.value = False

        self.after_stop()
        logger.info('worker is stopped!')

    def worker_routine(self, data):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')

    def before_start(self):
        """This method can be implemented in inherited class"""
        pass

    def after_stop(self):
        """This method can be implemented in inherited class"""
        pass


class ThreadBasedFriWorker(ThreadBasedAbstractWorker):
    def __init__(self, name, queue, key_storage=None):
        ThreadBasedAbstractWorker.__init__(self, name, queue)
        self._key_storage = key_storage
        self._ssl_context = None if not self._key_storage else self._key_storage.get_node_context()

    def worker_routine(self, socket):
        if self._key_storage:
            socket = Connection(self._ssl_context, socket)
            socket.setup_ssl()
            socket.set_accept_state()
            socket.accept_ssl()

        socket_proc = SocketProcessor(socket)

        try:
            self.process(socket_proc)
            socket_proc.close_socket()
        except Exception, err:
            socket_proc.close_socket(force=True)
            raise err

    def process(self, socket_processor):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')


class ProcessBasedFriWorker(ProcessBasedAbstractWorker):
    def __init__(self, name, queue, key_storage=None):
        ProcessBasedAbstractWorker.__init__(self, name, queue)
        self._key_storage = key_storage
        self._ssl_context = None if not self._key_storage else self._key_storage.get_node_context()

    def worker_routine(self, reduced_socket):
        fd = rebuild_handle(reduced_socket)
        sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        mp.forking.close(fd)

        if self._key_storage:
            sock = Connection(self._ssl_context, sock)
            sock.setup_ssl()
            sock.set_accept_state()
            sock.accept_ssl()

        socket_proc = SocketProcessor(sock)

        try:
            self.process(socket_proc)
            socket_proc.close_socket()
        except Exception, err:
            socket_proc.close_socket(force=True)
            raise err

    def process(self, socket_processor):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')

