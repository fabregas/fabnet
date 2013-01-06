import unittest
import time
import os
import logging
import threading
import json
import random
from fabnet.core import constants
constants.CHECK_NEIGHBOURS_TIMEOUT = 1
from fabnet.core.fri_server import FriServer
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.utils.logger import logger
from fabnet.core.node import Node

logger.setLevel(logging.DEBUG)

class TestNodeUpdateConfig(unittest.TestCase):
    def test_node(self):
        try:
            server = None
            address = '127.0.0.1:1987'
            server = Node('127.0.0.1', 1987, '/tmp', 'test_node',
                    ks_path=None, ks_passwd=None, node_type='BASE')

            server.start(None)
            time.sleep(.5)

            fri_client = FriClient()
            params = {}
            packet_obj = FabnetPacketRequest(method='UpdateNodeConfig', parameters=params, sync=True)
            resp = fri_client.call_sync(address, packet_obj)
            self.assertNotEqual(resp.ret_code, 0)

            params = {'config': {'TEST_CONFIG': 'str', 'TEST_INT': 234}}
            packet_obj = FabnetPacketRequest(method='UpdateNodeConfig', parameters=params, sync=True)
            resp = fri_client.call_sync(address, packet_obj)
            self.assertEqual(resp.ret_code, 0, resp.ret_message)

            packet_obj = FabnetPacketRequest(method='GetNodeConfig', sync=True)
            resp = fri_client.call_sync(address, packet_obj)
            self.assertEqual(resp.ret_code, 0, resp.ret_message)
            self.assertEqual(resp.ret_parameters['TEST_CONFIG'], 'str')
            self.assertEqual(resp.ret_parameters['TEST_INT'], 234)

            time.sleep(.2)
        except Exception, err:
            print 'ERROR: %s'%err
            raise err
        finally:
            if server:
                server.stop()


if __name__ == '__main__':
    unittest.main()

