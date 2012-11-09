import unittest
import time
import os
import logging
import shutil
import threading
import json
import random
import subprocess
import signal
import string
import hashlib
from datetime import datetime

from client.logger import logger

#logger.setLevel(logging.DEBUG)
from client import constants
constants.CHUNK_SIZE = 100000

from client.nibbler import Nibbler
from client.security_manager import init_security_manager

DEBUG=False

CLIENT_KS_PATH = './tests/cert/test_client_ks.zip'
VALID_STORAGE = './tests/cert/test_keystorage.zip'
PASSWD = 'qwerty123'

class SecurityManagerMock:
    def get_user_id(self):
        return 'this is test USER ID string'

    def get_client_cert(self):
        return 'fake cert'

    def get_client_cert_key(self):
        return

    def encrypt(self, data):
        return data

    def decrypt(self, data):
        return data


class TestDHTInitProcedure(unittest.TestCase):
    NODE_PROC = None
    NODE_ADDRESS = None
    NIBBLER_INST = None

    def dht_init(self, storage=None, passwd=None):
        n_node = 'init-fabnet'
        i = 1987
        address = '127.0.0.1:%s'%i

        home = '/tmp/node_%s'%i
        if os.path.exists(home):
            shutil.rmtree(home)
        os.mkdir(home)

        logger.warning('{SNP} STARTING NODE %s'%address)
        args = ['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, '%.02i'%i, home, 'DHT']
        if storage:
            args += [storage, passwd]
        if DEBUG:
            args.append('--debug')
        p = subprocess.Popen(args)
        logger.warning('{SNP} PROCESS STARTED')
        time.sleep(1.5)

        TestDHTInitProcedure.NODE_PROC = p
        TestDHTInitProcedure.NODE_ADDRESS = address

        if not storage:
            security_manager = SecurityManagerMock()
        else:
            security_manager = init_security_manager(CLIENT_KS_PATH, PASSWD)

        nibbler = Nibbler('127.0.0.1', security_manager)
        TestDHTInitProcedure.NIBBLER_INST = nibbler

        nibbler.register_user()

    def dht_stop(self):
        TestDHTInitProcedure.NIBBLER_INST.stop()
        TestDHTInitProcedure.NODE_PROC.send_signal(signal.SIGINT)
        time.sleep(2.5)
        home = '/tmp/node_1987'
        if os.path.exists(home):
            shutil.rmtree(home)

    def save_load_file(self, storage, passwd):
        self.dht_init(storage, passwd)
        try:
            nibbler = TestDHTInitProcedure.NIBBLER_INST
            fb = open('/tmp/test_file.out', 'wb')
            data = ''.join(random.choice(string.letters) for i in xrange(1024))
            data *= 2*1024
            fb.write(data)
            fb.close()
            checksum = hashlib.sha1(data).hexdigest()

            t0 = datetime.now()
            nibbler.save_file('/tmp/test_file.out', 'test_file.out', '/')

            for i in xrange(60):
                time.sleep(1)
                if nibbler.get_resource('/test_file.out'):
                    break
            else:
                raise Exception('File does not uploaded!')
            t1 = datetime.now()
            print 'File upload time: %s'%(t1-t0)

            os.remove('/tmp/test_file.out')

            #get file
            t0 = datetime.now()
            file_iterator = nibbler.load_file('/test_file.out')
            t1 = datetime.now()
            print 'File download time: %s'%(t1-t0)
            sha1 = hashlib.sha1()
            for data in file_iterator:
                sha1.update(data)

            self.assertEqual(sha1.hexdigest(), checksum)
        finally:
            self.dht_stop()


    def test01(self):
        print 'Unsecured file put/get...'
        self.save_load_file(None, None)

        print 'Secured file put/get...'
        self.save_load_file(VALID_STORAGE, PASSWD)

if __name__ == '__main__':
    unittest.main()

