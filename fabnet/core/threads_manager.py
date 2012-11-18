#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.threads_manager
@author Konstantin Andrusenko
@date November 16, 2012
"""
import time
import threading

from fabnet.utils.logger import logger
from fabnet.core.constants import STOP_THREAD_EVENT


class ThreadsManager(threading.Thread):
    def __init__(self, queue, min_count, max_count, workers_name, worker_class, init_params):
        threading.Thread.__init__(self)
        self.queue = queue
        self.min_count = min_count
        self.max_count = max_count
        self.workers_name = workers_name
        self.worker_class = worker_class
        self.init_params = init_params

        self.__threads = []
        self.__threads_idx = 0
        self.__lock = threading.Lock()
        self.stopped = True

    def get_workers_stat(self):
        act_count = busy_count = 0
        self.__lock.acquire()
        try:

            for thread in self.__threads:
                if thread.is_alive():
                    act_count += 1
                    is_busy = thread.is_busy()
                    if is_busy:
                        busy_count += 1
        finally:
            self.__lock.release()

        return act_count, busy_count

    def run(self):
        self.stopped = False
        for i in range(self.min_count):
            self.__spawn_work_threads()

        logger.info('Started %s work threads!'%self.min_count)
        not_empty_queue_count = 0
        empty_queue_count = 0

        while not self.stopped:
            try:
                time.sleep(.2)
                if self.queue.qsize() > 0:
                    not_empty_queue_count += 1
                    empty_queue_count = 0
                else:
                    not_empty_queue_count = 0
                    empty_queue_count += 1

                act, busy = self.get_workers_stat()
                if not_empty_queue_count >= 5:
                    if act == busy:
                        self.__spawn_work_threads()
                elif empty_queue_count >= 15:
                    if (act - busy) > self.min_count:
                        self.__stop_work_thread()
            except Exception, err:
                ret_message = '%s error: %s' % (self.getName(), err)
                logger.error(ret_message)

        logger.info('%s is stopped!'%self.getName())

    def __spawn_work_threads(self):
        logger.debug('starting new work thread')
        self.__lock.acquire()
        if self.stopped:
            return

        try:
            if len(self.__threads) == self.max_count:
                return

            thread = self.worker_class(*self.init_params)
            thread.setName('%s-%s#%02i' % (self.workers_name, self.__class__.__name__, self.__threads_idx))

            self.__threads_idx += 1
            thread.start()
            self.__threads.append(thread)
        finally:
            self.__lock.release()

    def __stop_work_thread(self):
        self.__lock.acquire()
        if self.stopped:
            return

        try:
            for_delete = []
            for i, thread in enumerate(self.__threads):
                if not thread.is_alive():
                    logger.debug('Worker %s is not alive! delete it...'%thread.getName())
                    for_delete.append(thread)

            for thr in for_delete:
                del self.__threads[self.__threads.index(thr)]

            if len(self.__threads) <= self.min_count:
                logger.debug('trying stopping worker but min threads count occured')
                return

            self.queue.put(STOP_THREAD_EVENT)
        finally:
            self.__lock.release()

        logger.debug('stopped one work thread')


    def stop(self):
        self.__lock.acquire()
        self.stopped = True
        try:
            act_count = 0
            for thread in self.__threads:
                if thread.is_alive():
                    act_count += 1

            for i in xrange(act_count):
                self.queue.put(STOP_THREAD_EVENT)

            #self.queue.join()

            for thread in self.__threads:
                if thread.is_alive():
                    thread.join()
        except Exception, err:
            logger.error('stopping error: %s'%err)
        finally:
            self.__lock.release()
