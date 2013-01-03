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
import multiprocessing as mp

from fabnet.utils.logger import logger
from fabnet.core.constants import STOP_WORKER_EVENT
from fabnet.core.socket_processor import SocketProcessor


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
                self.__busy_flag.clear()

        logger.info('worker is stopped!')

    def worker_routine(self, data):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')


class ProcessBasedAbstractWorker(mp.Process):
    is_threaded = False

    def __init__(self, name, queue):
        mp.Process.__init__(self)
        self.__is_busy = mp.Value("b", False, lock=mp.Lock())
        self.__queue = queue
        self.__name = name

    def is_busy(self):
        return self.__is_busy.value

    def run(self):
        cur_thread = threading.current_thread()
        cur_thread.setName(self.__name)

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
                self.__is_busy.value = False

        logger.info('worker is stopped!')

    def worker_routine(self, data):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')


class ThreadBasedFriWorker(ThreadBasedAbstractWorker):
    def worker_routine(self, socket):
        socket_proc = SocketProcessor(socket)

        try:
            self.process(socket_proc)
            socket_proc.close_socket()
        except Exception, err:
            socket_proc.close_socket(force=True)
            raise err

    def process(self, data):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')


class ProcessBasedFriWorker(ProcessBasedAbstractWorker):
    def worker_routine(self, reduced_socket):
        sock = reduced_socket[0](*reduced_socket[1])
        socket_proc = SocketProcessor(sock)

        try:
            self.process(socket_proc)
            socket_proc.close_socket()
        except Exception, err:
            socket_proc.close_socket(force=True)
            raise err

    def process(self, data):
        """This method must be implemented in inherited class"""
        raise RuntimeError('Not implemented')

