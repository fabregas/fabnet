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
from fabnet.core.fri_server import FriServer, FriClient, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.operator import Operator
from fabnet.operations.node_statistic import NodeStatisticOperation
from fabnet.utils.logger import logger

logger.setLevel(logging.DEBUG)

class TestNodeStatistic(unittest.TestCase):
    def test_node(self):
        try:
            server = None
            address = '127.0.0.1:1987'
            operator = Operator(address)

            operator.register_operation('NodeStatistic', NodeStatisticOperation)
            server = FriServer('0.0.0.0', 1987, operator, server_name='node_test')
            ret = server.start()
            self.assertEqual(ret, True)
            time.sleep(.5)

            packet = { 'message_id': 323232,
                        'method': 'NodeStatistic',
                        'sender': '',
                        'parameters': {},
                        'sync': True}
            packet_obj = FabnetPacketRequest(**packet)

            client = FriClient()
            ret_packet = client.call_sync('127.0.0.1:1987', packet_obj)

            self.assertEqual(isinstance(ret_packet, FabnetPacketResponse), True)
            self.assertEqual(ret_packet.ret_code, 0, ret_packet.ret_message)
            self.assertTrue(int(ret_packet.ret_parameters['workers_count']) > 2)
            self.assertEqual(int(ret_packet.ret_parameters['uppers_balance']), -1)
            self.assertEqual(int(ret_packet.ret_parameters['superiors_balance']), -1)
            self.assertTrue(int(ret_packet.ret_parameters['threads']) > 6)
            self.assertTrue(int(ret_packet.ret_parameters['memory']) > 1000)

            time.sleep(.2)
        except Exception, err:
            print 'ERROR: %s'%err
            raise err
        finally:
            if server:
                server.stop()


if __name__ == '__main__':
    unittest.main()

