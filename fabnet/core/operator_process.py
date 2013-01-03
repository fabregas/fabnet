#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator_process
@author Konstantin Andrusenko
@date January 03, 2013

This module contains the OperatorProcess class implementation
"""

import os
import pickle
import threading
import multiprocessing as mp
from Queue import Queue
from multiprocessing.connection import Listener, Client

from fabnet.core.workers_manager import WorkersManager
from fabnet.core.workers import ThreadBasedAbstractWorker
from fabnet.core.constants import OPERATOR_SOCKET_ADDRESS
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

            resp = method_f(*args)
            raw_resp = pickle.dumps({'rcode': 0, 'resp': resp})
            conn.send_bytes(raw_resp)
        except Exception, err:
            raw_resp = pickle.dumps({'rcode': 1, 'rmsg': str(err)})
            conn.send_bytes(raw_resp)
            raise err
        finally:
            conn.close()


class OperatorClient:
    def __init__(self, authkey=None):
        self.__authket = authkey

    def __getattr__(self, method):
        if method.startswith('_'):
            return None

        return lambda *args: self.__call(method, args)

    def __call(self, method, args=()):
        conn = None
        try:
            conn = Client(OPERATOR_SOCKET_ADDRESS, authkey=self.__authkey)
            raw_packet = pickle.dumps({'method': method, 'args': args})
            conn.send_bytes(raw_packet)
            raw_resp = conn.recv_bytes()
        except Exception, err:
            msg = 'Communication with operator process are failed! Details: %s'
            if not str(err):
                err = 'unknown'
            raise Exception(msg % err)
        finally:
            if conn:
                conn.close()

        resp = pickle.loads(raw_resp)
        if resp.get('rcode', 1) != 0:
            raise Exception('Operator method "%s" failed! Details: %s'% \
                                (method, resp.get('rmsg','unknown')))

        return resp.get('resp', None)




class OperatorProcess(mp.Process):
    def __init__(self, operator, server_name='', min_workers=2, max_workers=10, authkey=''):
        mp.Process.__init__(self)
        self.__is_stopped = mp.Value("b", True, lock=mp.Lock())
        self.__operator = operator
        self.__authkey = authkey
        self.__server_name = server_name
        self.__workers_mgr = WorkersManager(OperatorWorker, min_workers, max_workers, \
                    '%s-operator'%server_name, init_params=(self.__operator,))
        self.__queue = self.__workers_mgr.get_queue()

    def stop(self):
        self.__is_stopped.value = True
        logger.info('stopping operator...')
        try:
            conn = Client(OPERATOR_SOCKET_ADDRESS, authkey=self.__authkey)
            conn.send_bytes('bue!')
            conn.close()
        except EOFError:
            pass
        except Exception, err:
            logger.debug('[AbstractOperator.stop] connecting to operator is failed. details: %s'%err)

    def run(self):
        cur_thread = threading.current_thread()
        cur_thread.setName('%s-operator-main'%self.__server_name)

        logger.info('operator is started!')
        self.__is_stopped.value = False
        listener = None

        try:
            self.__workers_mgr.start()

            if os.path.exists(OPERATOR_SOCKET_ADDRESS):
                os.unlink(OPERATOR_SOCKET_ADDRESS)

            listener = Listener(OPERATOR_SOCKET_ADDRESS, authkey=self.__authkey)

            while True:
                conn = listener.accept()
                if self.__is_stopped.value:
                    conn.close()
                    break
                self.__queue.put(conn)
        except Exception, err:
            logger.error('AbstractOperator.listener failed: %s'%err)
        finally:
            if listener:
                listener.close()
            self.__is_stopped.value = True

        logger.info('operator is stopped!')

