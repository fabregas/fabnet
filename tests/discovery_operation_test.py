import unittest
import time
import os
import logging
import threading
import json
import random
from fabnet.utils.db_conn import SqliteDBConnection as DBConnection
from fabnet.core import constants
constants.CHECK_NEIGHBOURS_TIMEOUT = 1
from fabnet.core.fri_server import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.operator import Operator
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.utils.logger import logger
from fabnet.core.constants import NT_UPPER, NT_SUPERIOR

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
        operator = Operator(address, node_name=str(self.port))
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
        #os.system('rm /tmp/fabnet_topology.db')
        try:
            operator = Operator('127.0.0.1:1986', '1986')
            operator.register_operation('ManageNeighbour', ManageNeighbour)
            operator.register_operation('DiscoveryOperation', DiscoveryOperation)
            operator.register_operation('TopologyCognition', TopologyCognition)
            server1 = FriServer('0.0.0.0', 1986, operator, server_name='node_1')
            ret = server1.start()
            self.assertEqual(ret, True)

            operator1 = Operator('127.0.0.1:1987', '1987')
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
            operator1.call_node('127.0.0.1:1986', packet_obj)

            time.sleep(1)

            self.assertEqual(operator.get_neighbours(NT_UPPER), ['127.0.0.1:1987'])
            self.assertEqual(operator.get_neighbours(NT_SUPERIOR), ['127.0.0.1:1987'])
            self.assertEqual(operator1.get_neighbours(NT_UPPER), ['127.0.0.1:1986'])
            self.assertEqual(operator1.get_neighbours(NT_SUPERIOR), ['127.0.0.1:1986'])
        finally:
            if server1:
                server1.stop()
            if server2:
                server2.stop()


    def test_topology_cognition(self):
        servers = []
        addresses = []
        #os.system('rm /tmp/fabnet_topology.db')
        try:
            for i in range(1900, 1900+NODES_COUNT):
                address = '127.0.0.1:%s'%i
                server = TestServerThread(i)
                server.start()
                servers.append(server)

                time.sleep(1)

                if addresses:
                    part_address = random.choice(addresses)
                    packet =  { 'method': 'DiscoveryOperation',
                                'sender': address,
                                'parameters': {}}
                    packet_obj = FabnetPacketRequest(**packet)
                    server.operator.call_node(part_address, packet_obj)

                addresses.append(address)

            time.sleep(1)

            packet = {  'method': 'TopologyCognition',
                        'sender': None,
                        'parameters': {}}
            packet_obj = FabnetPacketRequest(**packet)
            servers[0].operator.call_network(packet_obj)

            time.sleep(1)


            conn = DBConnection('/tmp/fabnet_topology.db')
            conn.connect()
            rows = conn.select("SELECT node_address, node_name, superiors, uppers FROM fabnet_nodes")
            conn.close()
            nodes = {}
            for row in rows:
                nodes[row[0]] = row

            print 'NODES LIST: %s'%str(nodes)

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

