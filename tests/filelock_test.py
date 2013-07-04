import unittest
import time
import os
import logging
import threading
import json
import random

from fabnet.utils.filelock import *

#class Writer(threading.Thread):
#    def __init__(self, file_path):


class TestFileLock(unittest.TestCase):
    def test_lockedfile(self):
        fpath = '/tmp/test_locked_file'
        if os.path.exists(fpath):
            os.remove(fpath)

        f = LockedFile(fpath, exclusive=True, new_file=True)
        with self.assertRaises(AlreadyExists):
            LockedFile(fpath, exclusive=True, new_file=True)

        data = 'test message:\nHello, world!'
        f.append(data)
        f.close()

        f = LockedFile(fpath, exclusive=True)
        data1 = '\nThis text is appended to exists file...'
        f.append(data1)
        f.seek(4, os.SEEK_SET)
        f.write('_')
        f.close()
        data = data[:4] + '_' + data[5:]

        f = LockedFile(fpath)
        rdata = f.read()
        f.close()
        self.assertEqual(rdata, data+data1)

        f = LockedFile(fpath)
        rdata = f.read(5)
        rdata += f.read()
        f.close()
        self.assertEqual(rdata, data+data1)

if __name__ == '__main__':
    unittest.main()

