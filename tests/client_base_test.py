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

from client.logger import logger

logger.setLevel(logging.DEBUG)
from client import constants
constants.CHUNK_SIZE = 100000

from client.nibbler import Nibbler

DEBUG=False

class SecurityProviderMock:
    def get_user_id(self):
        return 'this is test USER ID string'

    def get_network_key(self):
        pass

    def encrypt(self, data):
        return data

    def decrypt(self, data):
        return data

class TestDHTInitProcedure(unittest.TestCase):
    NODE_PROC = None
    NODE_ADDRESS = None
    NIBBLER_INST = None

    def test01_dht_init(self):
        n_node = 'init-fabnet'
        i = 1987
        address = '127.0.0.1:%s'%i

        home = '/tmp/node_%s'%i
        if os.path.exists(home):
            shutil.rmtree(home)
        os.mkdir(home)

        logger.warning('{SNP} STARTING NODE %s'%address)
        args = ['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, '%.02i'%i, home]
        if DEBUG:
            args.append('--debug')
        p = subprocess.Popen(args)
        logger.warning('{SNP} PROCESS STARTED')
        time.sleep(1.5)

        TestDHTInitProcedure.NODE_PROC = p
        TestDHTInitProcedure.NODE_ADDRESS = address

        nibbler = Nibbler('127.0.0.1', SecurityProviderMock())
        TestDHTInitProcedure.NIBBLER_INST = nibbler

        nibbler.register_user()


    def test99_dht_stop(self):
        TestDHTInitProcedure.NIBBLER_INST.stop()
        TestDHTInitProcedure.NODE_PROC.send_signal(signal.SIGINT)
        time.sleep(2.5)
        home = '/tmp/node_1987'
        if os.path.exists(home):
            shutil.rmtree(home)

    def test02_create_dir(self):
        nibbler = TestDHTInitProcedure.NIBBLER_INST
        nibbler.mkdir('/my_first_dir')
        nibbler.mkdir('/my_second_dir')
        nibbler.mkdir('/my_first_dir/my_first_subdir')

        try:
            nibbler.mkdir('/my_first_dir')
        except Exception, err:
            pass
        else:
            raise Exception('should be exception in this case')

    def test03_save_file(self):
        nibbler = TestDHTInitProcedure.NIBBLER_INST
        fb = open('/tmp/test_file.out', 'wb')
        data = ''.join(random.choice(string.letters) for i in xrange(1024))
        data *= 10*1024
        fb.write(data)
        fb.close()
        checksum = hashlib.sha1(data).hexdigest()

        nibbler.save_file('/tmp/test_file.out', 'test_file.out', '/my_first_dir/my_first_subdir')

        for i in xrange(20):
            time.sleep(1)
            if nibbler.get_resource('/my_first_dir/my_first_subdir/test_file.out'):
                break
        else:
            raise Exception('File does not uploaded!')

        os.remove('/tmp/test_file.out')

        #get file
        file_iterator = nibbler.load_file('/my_first_dir/my_first_subdir/test_file.out')
        sha1 = hashlib.sha1()
        for data in file_iterator:
            sha1.update(data)

        self.assertEqual(sha1.hexdigest(), checksum)

    def test05_listdir(self):
        nibbler = TestDHTInitProcedure.NIBBLER_INST
        items = nibbler.listdir()
        self.assertEqual(len(items), 2, items)
        self.assertEqual(items[0], ('my_first_dir', False))
        self.assertEqual(items[1], ('my_second_dir', False))

        items = nibbler.listdir('/my_first_dir/my_first_subdir')
        self.assertEqual(len(items), 1, items)
        self.assertEqual(items[0], ('test_file.out', True))

    def test06_versions(self):
        nibbler = TestDHTInitProcedure.NIBBLER_INST
        versions = nibbler.get_versions()
        self.assertEqual(len(versions), 4)

        nibbler.load_version(versions[0][1])
        items = nibbler.listdir('/')
        self.assertEqual(len(items), 1, items)
        self.assertEqual(items[0], ('my_first_dir', False))

        nibbler.load_version(versions[-1][1])


    def test07_remove_file(self):
        nibbler = TestDHTInitProcedure.NIBBLER_INST

        nibbler.remove_file('/my_first_dir/my_first_subdir/test_file.out')
        items = nibbler.listdir('/my_first_dir/my_first_subdir')
        self.assertEqual(len(items), 0, items)


    def test08_rmdir(self):
        nibbler = TestDHTInitProcedure.NIBBLER_INST
        try:
            nibbler.rmdir('/my_first_dir')
        except Exception, err:
            pass
        else:
            raise Exception('should be exception in this case')

        nibbler.rmdir('/my_first_dir', recursive=True)
        items = nibbler.listdir()
        self.assertEqual(len(items), 1, items)
        self.assertEqual(items[0], ('my_second_dir', False))



if __name__ == '__main__':
    unittest.main()

