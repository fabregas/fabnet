#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.workers_manager
@author Konstantin Andrusenko
@date January 03, 2013
"""
import time
import threading
import multiprocessing as mp
from multiprocessing import Queue as ProcessQueue
from Queue import Queue as ThreadQueue


from fabnet.utils.logger import logger
from fabnet.core.constants import STOP_WORKER_EVENT, S_PENDING, S_ERROR, S_INWORK,\
                                    MIN_WORKERS_COUNT, MAX_WORKERS_COUNT


class WorkersManager(threading.Thread):
    def __init__(self, worker_class, min_count=MIN_WORKERS_COUNT, \
                    max_count=MAX_WORKERS_COUNT, server_name='srv', init_params=()):
        threading.Thread.__init__(self)
        self.min_count = min_count
        self.max_count = max_count
        self.server_name = server_name
        self.worker_class = worker_class
        self.init_params = init_params
        if self.worker_class.is_threaded:
            self.queue = ThreadQueue()
        else:
            self.queue = ProcessQueue()

        self.__workers = []
        self.__workers_idx = 0
        self.__lock = threading.Lock()
        self.__status = S_PENDING
        self.stopped = threading.Event()

    def get_queue(self):
        self.__lock.acquire()
        try:
            return self.queue
        finally:
            self.__lock.release()

    def get_workers_name(self):
        return self.worker_class.__name__

    def iter_children(self):
        self.__lock.acquire()
        try:
            for worker in self.__workers:
                if worker.is_alive():
                    yield worker
        finally:
            self.__lock.release()


    def get_workers_stat(self):
        act_count = busy_count = 0
        self.__lock.acquire()
        try:

            for worker in self.__workers:
                if worker.is_alive():
                    act_count += 1
                    is_busy = worker.is_busy()
                    if is_busy:
                        busy_count += 1
        finally:
            self.__lock.release()

        return act_count, busy_count

    def run(self):
        try:
            cur_thread = threading.current_thread()
            cur_thread.setName('%s-%s-Manager'%(self.server_name, self.worker_class.__name__))
            for i in range(self.min_count):
                self.__spawn_worker()

            self.__lock.acquire()
            self.__status = S_INWORK
            self.__lock.release()
        except Exception, err:
            self.__lock.acquire()
            self.__status = S_ERROR
            self.__lock.release()
            logger.error(err)
            import traceback
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            raise err

        logger.info('Started %s work threads!'%self.min_count)
        not_empty_queue_count = 0
        empty_queue_count = 0

        while not self.stopped.is_set():
            try:
                time.sleep(.5)
                act, busy = self.get_workers_stat()
                if act != busy:
                    not_empty_queue_count = 0
                    empty_queue_count += 1
                else:
                    not_empty_queue_count += 1
                    empty_queue_count = 0

                if not_empty_queue_count >= 4:
                    self.__spawn_worker()
                    not_empty_queue_count = 0
                elif empty_queue_count >= 60:
                    self.__stop_worker()
                    empty_queue_count = 0
            except Exception, err:
                ret_message = 'run() error: %s' % err
                logger.error(ret_message)

        logger.info('workers manager is stopped!')

    def __spawn_worker(self):
        logger.debug('starting new worker')
        if self.stopped.is_set():
            return

        self.__lock.acquire()
        try:
            if len(self.__workers) == self.max_count:
                return

            worker_name = '%s-%s#%02i' % (self.server_name, self.worker_class.__name__, self.__workers_idx)
            worker = self.worker_class(worker_name, self.queue, *self.init_params)

            self.__workers_idx += 1
            worker.start()
            self.__workers.append(worker)
        finally:
            self.__lock.release()

    def __stop_worker(self):
        if self.stopped.is_set():
            return

        self.__lock.acquire()
        try:
            for_delete = []
            for i, worker in enumerate(self.__workers):
                if not worker.is_alive():
                    for_delete.append(worker)

            for thr in for_delete:
                del self.__workers[self.__workers.index(thr)]

            if len(self.__workers) <= self.min_count:
                logger.debug('trying stopping worker but min workers count occured')
                return

            self.queue.put(STOP_WORKER_EVENT)
        finally:
            self.__lock.release()

        logger.debug('stopped one worker')


    def stop(self):
        logger.info('stopping workers manager for %s...'%self.worker_class.__name__)
        self.stopped.set()
        self.__lock.acquire()
        try:
            act_count = 0
            for worker in self.__workers:
                if worker.is_alive():
                    act_count += 1

            for i in xrange(act_count):
                self.queue.put(STOP_WORKER_EVENT)

            for worker in self.__workers:
                if worker.is_alive():
                    worker.join()
        except Exception, err:
            logger.error('stopping error: %s'%err)
        finally:
            self.__lock.release()

        logger.info('workers manager for %s is stopped!'%self.worker_class.__name__)


    def start_carefully(self):
        self.start()
        while True:
            self.__lock.acquire()
            try:
                if self.__status != S_PENDING:
                    break
            finally:
                self.__lock.release()

            time.sleep(.1)

        if self.__status == S_ERROR:
            manager_name = '%s-%s-Manager'%(self.server_name, self.worker_class.__name__)
            raise Exception('%s does not started!!!'%manager_name)

