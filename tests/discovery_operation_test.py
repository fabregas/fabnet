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
from fabnet.core.fri_base import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.operator import Operator
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.utils.logger import logger

logger.setLevel(logging.DEBUG)

NODES_COUNT = 3

class TestServerThread(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.stopped = True
        self.operator = None

    def run(self):
        address = '127.0.0.1:%s'%self.port
        operator = Operator(address)
        self.operator = operator

        operator.register_operation('ManageNeighbour', ManageNeighbour)
        operator.register_operation('DiscoveryOperation', DiscoveryOperation)
        operator.register_operation('TopologyCognition', TopologyCognition)
        server = FriServer('0.0.0.0', self.port, operator, server_name='node_%s'%self.port)
        ret = server.start()
        if not ret:
            return

        self.stopped = False

        while not self.stopped:
            time.sleep(0.1)

        server.stop()

    def stop(self):
        self.stopped = True




class TestDiscoverytOperation(unittest.TestCase):
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

            self.assertEqual(operator.upper_neighbours, ['127.0.0.1:1987'])
            self.assertEqual(operator.superior_neighbours, ['127.0.0.1:1987'])
            self.assertEqual(operator1.upper_neighbours, ['127.0.0.1:1986'])
            self.assertEqual(operator1.superior_neighbours, ['127.0.0.1:1986'])
        finally:
            if server1:
                server1.stop()
            if server2:
                server2.stop()


    def test_topology_cognition(self):
        servers = []
        addresses = []
        try:
            for i in range(1900, 1900+NODES_COUNT):
                address = '127.0.0.1:%s'%i
                server = TestServerThread(i)
                server.start()
                servers.append(server)

                time.sleep(.5)

                if addresses:
                    part_address = random.choice(addresses)
                    packet =  { 'method': 'DiscoveryOperation',
                                'sender': address,
                                'parameters': {}}
                    packet_obj = FabnetPacketRequest(**packet)
                    rcode, rmsg = server.operator.call_node(part_address, packet_obj)
                    self.assertEqual(rcode, 0, rmsg)

                addresses.append(address)

            time.sleep(1)

            packet = {  'method': 'TopologyCognition',
                        'sender': None,
                        'parameters': {}}
            packet_obj = FabnetPacketRequest(**packet)
            rcode, rmsg = servers[0].operator.call_network(packet_obj)
            self.assertEqual(rcode, 0, rmsg)

            time.sleep(1)


            conn = sqlite3.connect('/tmp/fabnet_topology.db')

            curs = conn.cursor()
            curs.execute("SELECT node_address, node_name, superiors, uppers FROM fabnet_nodes")
            rows = curs.fetchall()
            conn.commit()

            curs.close()
            conn.close()
            nodes = {}
            for row in rows:
                nodes[row[0]] = row


            for i in range(1900, 1900+NODES_COUNT):
                address = '127.0.0.1:%s'%i
                self.assertTrue(nodes.has_key(address))
                self.assertEqual(nodes[address][1], 'node_%s'%i)
        finally:
            for server in servers:
                if server:
                    server.stop()

            for server in servers:
                if server:
                    server.join()


if __name__ == '__main__':
    unittest.main()

