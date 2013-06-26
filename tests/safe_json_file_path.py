import unittest
import time
import os
import logging
import json
import threading

from fabnet.utils.logger import logger
from fabnet.utils.safe_json_file import SafeJsonFile
from datetime import datetime
from multiprocessing import Process
from threading import Thread
from Queue import Queue

from fabnet.core.operator import OperatorProcess, OperatorClient

#logger.setLevel(logging.DEBUG)

class ReadThread(threading.Thread):
    def __init__(self, filepath, obj, i=100):
        threading.Thread.__init__(self)
        self.filepath = filepath
        self.obj = obj
        self.cnt = xrange(i)
        self.errors = []

    def run(self):
        for i in self.cnt:
            f = SafeJsonFile(self.filepath)
            try:
                data = f.read()
                if not data:
                    continue
                #if data != self.obj:
                #    raise Exception('invalid data read!')
                time.sleep(.05)
            except Exception, err:
                print 'error: %s'%err
                self.errors.append(err)

class WriteThread(threading.Thread):
    def __init__(self, filepath, obj, i=50):
        threading.Thread.__init__(self)
        self.filepath = filepath
        self.obj = obj
        self.cnt = xrange(i)
        self.errors = []

    def run(self):
        for i in self.cnt:
            f = SafeJsonFile(self.filepath)
            try:
                f.write(self.obj)
                time.sleep(.1)
            except Exception, err:
                self.errors.append(str(err))
 

class TestSafeJsonFile(unittest.TestCase):
    def test00_test_echo(self):
        threads = []
        file_path = '/tmp/test_safe_json'
        for i in xrange(10):
            obj = {'item': 'string item #%s'%i, 'int_val': i, 'dump_s': ('12323534%i564'%i)*1000}
            threads.append(WriteThread(file_path, obj)) 
            threads.append(ReadThread(file_path, obj)) 

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        os.system('rm /tmp/test_safe_json')
        for thread in threads:
            self.assertEqual(thread.errors, [])



if __name__ == '__main__':
    unittest.main()

