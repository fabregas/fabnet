import unittest
import time
import os
import logging
import threading
import json
import random
import  sqlite3
from fabnet.core import constants
constants.CHECK_NEIGHBOURS_TIMEOUT = 1
from fabnet.core.fri_server import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.operator import Operator
from fabnet.utils.logger import logger

logger.setLevel(logging.DEBUG)

class TestNodeStatistic(unittest.TestCase):
    def test_node(self):
        try:
            server = None
            address = '127.0.0.1:1987'
            operator = Operator(address)

            server = FriServer('0.0.0.0', 1987, operator, server_name='node_test')
            ret = server.start()
            self.assertEqual(ret, True)
            time.sleep(.5)

            packet = { 'message_id': 323232,
                        'method': 'NodeStatistic',
                        'sender': '',
                        'parameters': {'reset_op_stat': True},
                        'sync': True}
            packet_obj = FabnetPacketRequest(**packet)

            client = FriClient()
            ret_packet = client.call_sync('127.0.0.1:1987', packet_obj)

            self.assertEqual(isinstance(ret_packet, FabnetPacketResponse), True)
            self.assertEqual(ret_packet.ret_code, 0, ret_packet.ret_message)
            print ret_packet.ret_parameters
            self.assertTrue(int(ret_packet.ret_parameters['workers_count']) > 2)
            self.assertEqual(int(ret_packet.ret_parameters['uppers_balance']), -1)
            self.assertEqual(int(ret_packet.ret_parameters['superiors_balance']), -1)
            self.assertTrue(int(ret_packet.ret_parameters['threads']) > 6)
            self.assertTrue(int(ret_packet.ret_parameters['memory']) > 1000)
            self.assertEqual(len(ret_packet.ret_parameters['methods_stat']), 8)
            self.assertEqual(ret_packet.ret_parameters['agents_count']>2, True)

            self.assertEqual(ret_packet.ret_parameters['methods_stat']['NodeStatistic']['call_cnt'], 0)
            self.assertEqual(ret_packet.ret_parameters['methods_stat']['NodeStatistic']['avg_proc_time'], '0')

            time.sleep(.2)
        finally:
            if server:
                server.stop()


if __name__ == '__main__':
    unittest.main()

