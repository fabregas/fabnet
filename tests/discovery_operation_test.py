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
from fabnet.core.fri_server import FriServer
from fabnet.core.fri_client import FriClient
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.node import Node
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.utils.logger import logger
from fabnet.core.constants import NT_UPPER, NT_SUPERIOR
from fabnet.core.operator import OperatorClient

#logger.setLevel(logging.DEBUG)

NODES_COUNT = 5

class TestServerThread(threading.Thread):
    def __init__(self, port, neighbour=None):
        threading.Thread.__init__(self)
        self.port = port
        self.stopped = True
        self.node = None
        self.neighbour = neighbour

    def run(self):
        address = '127.0.0.1:%s'%self.port
        home_dir = '/tmp/node_%s'%self.port
        if os.path.exists(home_dir):
            os.system('rm -rf %s'%home_dir)
        os.mkdir(home_dir)
        node_name = 'node%s'%self.port

        node = Node('127.0.0.1', self.port, home_dir, node_name,
                    ks_path=None, ks_passwd=None, node_type='BASE')

        node.start(self.neighbour)
        self.node = node

        self.stopped = False

        while not self.stopped:
            time.sleep(0.1)

        node.stop()

    def stop(self):
        self.stopped = True




class TestDiscoverytOperation(unittest.TestCase):
    def test_discovery_operation(self):
        server1 = server2 = server3 = None
        #os.system('rm /tmp/fabnet_topology.db')
        try:
            server1 = TestServerThread(1986)
            server1.start()
            server2 = TestServerThread(1987, '127.0.0.1:1986')
            time.sleep(1)
            server2.start()
            server3 = TestServerThread(1988, '127.0.0.1:1986')
            time.sleep(1)
            server3.start()

            time.sleep(2)

            operator = OperatorClient('node1986')
            operator1 = OperatorClient('node1987')
            operator2 = OperatorClient('node1988')

            self.assertEqual(sorted(operator.get_neighbours(NT_UPPER)), ['127.0.0.1:1987', '127.0.0.1:1988'])
            self.assertEqual(sorted(operator.get_neighbours(NT_SUPERIOR)), ['127.0.0.1:1987', '127.0.0.1:1988'])
            self.assertEqual(sorted(operator1.get_neighbours(NT_UPPER)), ['127.0.0.1:1986', '127.0.0.1:1988'])
            self.assertEqual(sorted(operator1.get_neighbours(NT_SUPERIOR)), ['127.0.0.1:1986', '127.0.0.1:1988'])
            self.assertEqual(sorted(operator2.get_neighbours(NT_UPPER)), ['127.0.0.1:1986', '127.0.0.1:1987'])
            self.assertEqual(sorted(operator2.get_neighbours(NT_SUPERIOR)), ['127.0.0.1:1986', '127.0.0.1:1987'])

            server1.stop()
            server1 = None
            time.sleep(1)
            self.assertEqual(operator1.get_neighbours(NT_UPPER), ['127.0.0.1:1988'])
            self.assertEqual(operator1.get_neighbours(NT_SUPERIOR), ['127.0.0.1:1988'])
            self.assertEqual(operator2.get_neighbours(NT_UPPER), ['127.0.0.1:1987'])
            self.assertEqual(operator2.get_neighbours(NT_SUPERIOR), ['127.0.0.1:1987'])
        finally:
            try:
                if server1:
                    server1.stop()
            except Exception, err:
                print 'ERROR while stopping server1: %s'%err
            time.sleep(1)
            try:
                if server2:
                    server2.stop()
            except Exception, err:
                print 'ERROR while stopping server2: %s'%err
            time.sleep(1)
            try:
                if server3:
                    server3.stop()
            except Exception, err:
                print 'ERROR while stopping server3: %s'%err

    def test_topology_cognition(self):
        servers = []
        addresses = []
        #os.system('rm /tmp/fabnet_topology.db')
        try:
            for i in range(1900, 1900+NODES_COUNT):
                address = '127.0.0.1:%s'%i
                if not addresses:
                    neighbour = None
                else:
                    neighbour = random.choice(addresses)

                server = TestServerThread(i, neighbour)
                server.start()
                servers.append(server)
                addresses.append(address)
                time.sleep(1.5)

            time.sleep(1)

            packet = {  'method': 'TopologyCognition',
                        'sender': None,
                        'parameters': {}}
            packet_obj = FabnetPacketRequest(**packet)
            fri_client = FriClient()
            addr = random.choice(addresses)
            fri_client.call(addr, packet_obj)

            time.sleep(1)
            operator = OperatorClient('node%s'%addr.split(':')[-1])
            home_dir = operator.get_home_dir()

            conn = DBConnection(os.path.join(home_dir, 'fabnet_topology.db'))
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
                self.assertEqual(nodes[address][1], 'node%s'%i)
        finally:
            for server in servers:
                if server:
                    server.stop()
                    time.sleep(1)


if __name__ == '__main__':
    unittest.main()

