#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator.operator_process
@author Konstantin Andrusenko
@date January 03, 2013

This module contains the OperatorProcess class implementation
"""

import os
import pickle
import time
import threading
import multiprocessing as mp
from Queue import Queue
from multiprocessing.connection import Listener, Client

from fabnet.core.workers_manager import WorkersManager
from fabnet.core.workers import ThreadBasedAbstractWorker
from fabnet.core.constants import OPERATOR_SOCKET_ADDRESS, \
                            S_PENDING, S_ERROR, S_INWORK, \
                            MIN_WORKERS_COUNT, MAX_WORKERS_COUNT
from fabnet.utils.logger import logger

class OperatorWorker(ThreadBasedAbstractWorker):
    def __init__(self, name, queue, operator):
        ThreadBasedAbstractWorker.__init__(self, name, queue)
        self.__operator = operator

    def worker_routine(self, conn):
        try:
            pickled_data = conn.recv_bytes()
            raw_packet = pickle.loads(pickled_data)
            method = raw_packet.get('method', None)
            if not method:
                raise Exception('Method name does not found!')
            args = raw_packet.get('args', ())

            method_f = getattr(self.__operator, method, None)
            if not method_f:
                raise Exception('Unknown method "%s"'%method)

            #logger.debug('rpc call %s%s'%(method, args))

            resp = method_f(*args)
            raw_resp = pickle.dumps({'rcode': 0, 'resp': resp})
            conn.send_bytes(raw_resp)
        except Exception, err:
            logger.error('operator error: "%s"'%err.__class__.__name__)
            raw_resp = pickle.dumps({'rcode': 1, 'rmsg': str(err)})
            conn.send_bytes(raw_resp)
            raise err
        finally:
            conn.close()


class OperatorClient:
    def __init__(self, server_name, authkey=None):
        self.__authket = authkey
        self.__server_name = server_name

    def __getattr__(self, method):
        if method.startswith('_'):
            return None

        return lambda *args: self.__call(method, args)

    def __call(self, method, args=()):
        conn = None
        addr = OPERATOR_SOCKET_ADDRESS%self.__server_name
        try:
            raw_packet = pickle.dumps({'method': method, 'args': args})
            conn = Client(addr, authkey=self.__authkey)
            conn.send_bytes(raw_packet)
            raw_resp = conn.recv_bytes()
        except Exception, err:
            msg = 'Communication with operator process at address %s are failed! Details: %s'
            if not str(err):
                err = 'unknown'
            raise Exception(msg % (addr, err))
        finally:
            if conn:
                conn.close()

        resp = pickle.loads(raw_resp)
        if resp.get('rcode', 1) != 0:
            raise Exception('Operator method "%s" failed! Details: %s'% \
                                (method, resp.get('rmsg','unknown')))

        return resp.get('resp', None)




class OperatorProcess(mp.Process):
    def __init__(self, operator_class, self_address, home_dir, keystore, is_init_node, server_name='',\
                    min_workers=MIN_WORKERS_COUNT, max_workers=MAX_WORKERS_COUNT, authkey=''):
        mp.Process.__init__(self)
        self.__is_stopped = mp.Value("b", True, lock=mp.Lock())
        self.__status =  mp.Value("i", S_PENDING, lock=mp.Lock())
        self.__operator_class = operator_class
        self.__operator_args = (self_address, home_dir, keystore, is_init_node, server_name)

        self.__authkey = authkey
        self.__server_name = server_name
        self.__min_workers = min_workers
        self.__max_workers = max_workers
        self.__sock_addr = OPERATOR_SOCKET_ADDRESS % self.__server_name

    def stop(self):
        self.__is_stopped.value = True
        try:
            conn = Client(self.__sock_addr, authkey=self.__authkey)
            conn.send_bytes('bue!')
            conn.close()
        except EOFError:
            pass
        except Exception, err:
            logger.debug('[AbstractOperator.stop] connecting to operator is failed. details: %s'%err)

    def run(self):
        cur_thread = threading.current_thread()
        cur_thread.setName('%s-operator-main'%self.__server_name)

        self.__is_stopped.value = False
        listener = None

        try:
            self.__operator = self.__operator_class(*self.__operator_args)
            self.__workers_mgr = WorkersManager(OperatorWorker, self.__min_workers, self.__max_workers, \
                                    self.__server_name+'-op', init_params=(self.__operator,))
            self.__workers_mgr.start_carefully()
            self.__operator.set_operator_api_workers_manager(self.__workers_mgr)
            self.__queue = self.__workers_mgr.get_queue()

            if os.path.exists(self.__sock_addr):
                os.unlink(self.__sock_addr)

            listener = Listener(self.__sock_addr, authkey=self.__authkey)
            logger.debug('listen unix socket at %s'%self.__sock_addr)
            self.__status.value = S_INWORK

            logger.info('operator listener is started!')

            while True:
                conn = listener.accept()
                if self.__is_stopped.value:
                    conn.close()
                    break
                self.__queue.put(conn)
        except Exception, err:
            self.__status.value = S_ERROR
            logger.error('OperatorProcess failed: %s'%err)

        try:
            if listener:
                listener.close()
            self.__operator.stop()
            self.__workers_mgr.stop()
        except Exception, err:
            logger.error('Error while stopping Operator process. Details: %s'%err)
        finally:
            self.__is_stopped.value = True


        logger.info('operator listener is stopped!')

    def start_carefully(self):
        self.start()
        while self.__status.value == S_PENDING:
            time.sleep(.1)

        if self.__status.value == S_ERROR:
            raise Exception('Operator process does not started!!!')

