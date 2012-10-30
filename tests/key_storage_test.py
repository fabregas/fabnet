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

from fabnet.core.key_storage import *
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

VALID_STORAGE = './tests/cert/test_keystorage.zip'
INVALID_STORAGE = './tests/cert/test_keystorage_invalid.zip'

PASSWD = 'qwerty123'

class TestKeyStorage(unittest.TestCase):
    def test01_valid_storage(self):
        ks = FileBasedKeyStorage(VALID_STORAGE, PASSWD)
        inv_ks = FileBasedKeyStorage(INVALID_STORAGE, PASSWD)

        role = ks.verify_cert(ks.get_node_cert())
        self.assertEqual(role, NODE_ROLE)

        try:
            role = ks.verify_cert(inv_ks.get_node_cert())
        except Exception, err:
            pass
        else:
            raise Exception('should be exception in this case')

        context = ks.get_node_context()
        self.assertNotEqual(context, None)


if __name__ == '__main__':
    unittest.main()

