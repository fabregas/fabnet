import unittest
import time
import os
import logging
import json
import threading
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_server import FriServer
from fabnet.core.fri_client import FriClient
from fabnet.core.workers_manager import WorkersManager
from fabnet.core.workers import ProcessBasedFriWorker, ThreadBasedFriWorker
from fabnet.core.key_storage import FileBasedKeyStorage
from fabnet.utils.logger import logger
from datetime import datetime
from multiprocessing import Process
from threading import Thread
from Queue import Queue

from fabnet.core.operator_process import OperatorProcess, OperatorClient

logger.setLevel(logging.DEBUG)


class Operator:
    def echo(self, message):
        return message

    def test_ret_struct(self, str_arg, int_param):
        return ('str_param=%s, int_param=%i'%(str_arg, int_param), {'s':str_arg, 'i':int_param})

    def sleep(self, seconds):
        time.sleep(seconds)


class TestAbstractOperator(unittest.TestCase):
    def test00_test_echo(self):
        def echo_worker(name, err_queue):
            cl = OperatorClient()
            msg = 'Hello, I am %s'%name
            for i in xrange(500):
                ret_msg = cl.echo(msg)
                if ret_msg != msg:
                    error = '%s != %s'%(ret_msg, msg)
                    print 'ERROR: %s'%error
                    err_queue.put(error)

        proc = OperatorProcess(Operator(), server_name='test_node')
        proc.start()
        time.sleep(1)
        try:
            processes = []
            errors = Queue()
            for i in xrange(4):
                p = Thread(target=echo_worker, args=('worker #%i'%i, errors))
                processes.append(p)

            t0 = datetime.now()
            for p in processes:
                p.start()

            for p in processes:
                p.join()

            if not errors.empty():
                raise Exception('ERROR in threads...')
            print 'stress time: %s'%(datetime.now()-t0)
        finally:
            proc.stop()
            proc.join()


    def test01_ret_struct(self):
        proc = OperatorProcess(Operator(), server_name='test_node')
        proc.start()
        time.sleep(1)
        try:
            cl = OperatorClient()
            ret = cl.test_ret_struct('test message', 123456)
            self.assertEqual(len(ret), 2)
            self.assertEqual(ret[0], 'str_param=test message, int_param=123456')
            self.assertEqual(ret[1],  {'s':'test message', 'i':123456})
        finally:
            proc.stop()
            proc.join()

if __name__ == '__main__':
    unittest.main()

