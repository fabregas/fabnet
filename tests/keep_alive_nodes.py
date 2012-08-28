import unittest
import time
import os
import logging
import json
import random
from fabnet.core import constants
constants.CHECK_NEIGHBOURS_TIMEOUT = .1
constants.KEEP_ALIVE_MAX_WAIT_TIME = 0.5

from fabnet.core.fri_base import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.operator_base import Operator, OperationBase
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.utils.logger import logger

logger.setLevel(logging.DEBUG)



class TestKeepAlive(unittest.TestCase):
    def test_discovery_operation(self):
        server1 = server2 = None
        try:
            operator = Operator('127.0.0.1:1986')
            operator.register_operation('ManageNeighbour', ManageNeighbour)
            operator.register_operation('DiscoveryOperation', DiscoveryOperation)
            operator.register_operation('TopologyCognition', TopologyCognition)
            server1 = FriServer('0.0.0.0', 1986, operator, server_name='node_1')
            ret = server1.start()
            self.assertEqual(ret, True)

            operator1 = Operator('127.0.0.1:1987')
            operator1.register_operation('ManageNeighbour', ManageNeighbour)
            operator1.register_operation('DiscoveryOperation', DiscoveryOperation)
            operator1.register_operation('TopologyCognition', TopologyCognition)
            server2 = FriServer('0.0.0.0', 1987, operator1, server_name='node_2')
            ret = server2.start()
            self.assertEqual(ret, True)

            packet = { 'message_id': 323232,
                        'method': 'DiscoveryOperation',
                        'sender': '127.0.0.1:1987',
                        'parameters': {}}
            packet_obj = FabnetPacketRequest(**packet)
            rcode, rmsg = operator1.call_node('127.0.0.1:1986', packet_obj)
            self.assertEqual(rcode, 0, rmsg)

            time.sleep(1)
            server1.stop()
            time.sleep(1)
            server1 = None

            self.assertEqual(operator.upper_neighbours, ['127.0.0.1:1987'])
            self.assertEqual(operator.superior_neighbours, ['127.0.0.1:1987'])
            self.assertEqual(operator1.upper_neighbours, [])
            self.assertEqual(operator1.superior_neighbours, [])
        finally:
            if server1:
                server1.stop()
            if server2:
                server2.stop()



if __name__ == '__main__':
    unittest.main()

