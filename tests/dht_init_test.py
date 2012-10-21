import unittest
import time
import os
import logging
import shutil
import threading
import json
import random
from fabnet.core.fri_server import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER
from fabnet.dht_mgmt import constants
constants.WAIT_RANGE_TIMEOUT = 0.1
constants.INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT = 0.1
constants.MONITOR_DHT_RANGES_TIMEOUT = 1
constants.CHECK_HASH_TABLE_TIMEOUT = 1
constants.WAIT_FILE_MD_TIMEDELTA = 0.1
constants.WAIT_DHT_TABLE_UPDATE = .2
from fabnet.dht_mgmt.dht_operator import DHTOperator
from fabnet.dht_mgmt import dht_operator
from fabnet.dht_mgmt.operations import split_range_request
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.dht_mgmt.operations.get_range_data_request import GetRangeDataRequestOperation
from fabnet.dht_mgmt.operations.get_ranges_table import GetRangesTableOperation
from fabnet.dht_mgmt.operations.put_data_block import PutDataBlockOperation
from fabnet.dht_mgmt.operations.split_range_cancel import SplitRangeCancelOperation
from fabnet.dht_mgmt.operations.split_range_request import SplitRangeRequestOperation
from fabnet.dht_mgmt.operations.update_hash_range_table import UpdateHashRangeTableOperation
from fabnet.dht_mgmt.operations.check_hash_range_table import CheckHashRangeTableOperation

from fabnet.utils.logger import logger
from fabnet.dht_mgmt.constants import DS_NORMALWORK

logger.setLevel(logging.DEBUG)

MAX_HASH = constants.MAX_HASH

class TestServerThread(threading.Thread):
    def __init__(self, port, home_dir, init_node=False):
        threading.Thread.__init__(self)
        self.port = port
        self.home_dir = home_dir
        self.stopped = True
        self.operator = None
        self.init_node = init_node

    def run(self):
        address = '127.0.0.1:%s'%self.port
        operator = DHTOperator(address, self.home_dir, is_init_node=self.init_node, node_name=self.port)
        self.operator = operator

        operator.register_operation('ManageNeighbour', ManageNeighbour)
        operator.register_operation('DiscoveryOperation', DiscoveryOperation)
        operator.register_operation('TopologyCognition', TopologyCognition)

        operator.register_operation('GetRangeDataRequest', GetRangeDataRequestOperation)
        operator.register_operation('GetRangesTable', GetRangesTableOperation)
        operator.register_operation('PutDataBlock', PutDataBlockOperation)
        operator.register_operation('SplitRangeCancel', SplitRangeCancelOperation)
        operator.register_operation('SplitRangeRequest', SplitRangeRequestOperation)
        operator.register_operation('UpdateHashRangeTable', UpdateHashRangeTableOperation)
        operator.register_operation('CheckHashRangeTable', CheckHashRangeTableOperation)
        server = FriServer('0.0.0.0', self.port, operator, server_name='node_%s'%self.port)
        ret = server.start()
        if not ret:
            print 'ERROR: server does not started!'
            return

        self.stopped = False

        while not self.stopped:
            time.sleep(0.1)

        server.stop()

    def stop(self):
        self.stopped = True



class TestDHTInitProcedure(unittest.TestCase):
    def test01_dht_init(self):
        server = server1 = None
        try:
            home1 = '/tmp/node_1986_home'
            home2 = '/tmp/node_1987_home'
            if os.path.exists(home1):
                shutil.rmtree(home1)
            os.mkdir(home1)
            if os.path.exists(home2):
                shutil.rmtree(home2)
            os.mkdir(home2)

            server = TestServerThread(1986, home1, init_node=True)
            server.start()

            server1 = TestServerThread(1987, home2)
            server1.start()

            time.sleep(1)
            self.assertNotEqual(server1.operator.status, DS_NORMALWORK)

            server1.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:8080')
            time.sleep(.1)
            server1.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:1986')
            for i in range(10):
                if server1.operator.status == DS_NORMALWORK:
                    break
                time.sleep(1)
            else:
                raise Exception('Server1 does not started!')

            node86_range = server.operator.get_dht_range()
            node87_range = server1.operator.get_dht_range()

            self.assertEqual(node86_range.get_start(), 0L)
            self.assertEqual(node86_range.get_end(), MAX_HASH/2)
            self.assertEqual(node87_range.get_start(), MAX_HASH/2+1)
            self.assertEqual(node87_range.get_end(), MAX_HASH)

            table = server.operator.ranges_table.copy()
            self.assertEqual(len(table), 2)
            self.assertEqual(table[0].start, 0)
            self.assertEqual(table[0].end, MAX_HASH/2)
            self.assertEqual(table[1].start, MAX_HASH/2+1)
            self.assertEqual(table[1].end, MAX_HASH)

            self.assertEqual(server1.operator.status, DS_NORMALWORK)

            node86_range.put(MAX_HASH-100500, 'Hello, fabregas!') #should be appended into reservation range
            node87_range.put_replica(100, 'This is replica data!')
            time.sleep(1.5)
            data = node87_range.get(MAX_HASH-100500)
            self.assertEqual(data, 'Hello, fabregas!')
            data = node86_range.get_replica(100)
            self.assertEqual(data, 'This is replica data!')
            try:
                node87_range.get_replica(100)
            except Exception, err:
                pass
            else:
                raise Exception('should be exception in this case')

        finally:
            if server:
                server.stop()
                server.join()
            if server1:
                server1.stop()
                server1.join()

    def test02_dht_init_fail(self):
        server = server1 = None
        try:
            home1 = '/tmp/node_1986_home'
            home2 = '/tmp/node_1987_home'
            if os.path.exists(home1):
                shutil.rmtree(home1)
            os.mkdir(home1)
            if os.path.exists(home2):
                shutil.rmtree(home2)
            os.mkdir(home2)

            server = TestServerThread(1986, home1, init_node=True)
            server.start()

            server1 = TestServerThread(1987, home2)
            server1.start()

            time.sleep(1)
            self.assertNotEqual(server1.operator.status, DS_NORMALWORK)
            dht_range = server.operator.get_dht_range()
            dht_operator.DHT_CYCLE_TRY_COUNT = 10
            split_range_request.ALLOW_FREE_SIZE_PERCENTS = 0
            dht_range.put(MAX_HASH/2+100, 'Hello, fabregas! '*100)
            dht_range.split_range(0, 100500)


            server1.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:1986')
            time.sleep(.2)
            dht_range.join_subranges()
            time.sleep(.2)

            split_range_request.ALLOW_FREE_SIZE_PERCENTS = 70
            time.sleep(1)

            node86_range = server.operator.get_dht_range()
            node87_range = server1.operator.get_dht_range()
            self.assertEqual(node86_range.get_start(), 0L)
            self.assertEqual(node86_range.get_end(), MAX_HASH/2)
            self.assertEqual(node87_range.get_start(), MAX_HASH/2+1)
            self.assertEqual(node87_range.get_end(), MAX_HASH)

            table = server.operator.ranges_table.copy()
            self.assertEqual(len(table), 2)
            self.assertEqual(table[0].start, 0)
            self.assertEqual(table[0].end, MAX_HASH/2)
            self.assertEqual(table[1].start, MAX_HASH/2+1)
            self.assertEqual(table[1].end, MAX_HASH)

            self.assertEqual(server1.operator.status, DS_NORMALWORK)
        finally:
            if server:
                server.stop()
                server.join()
            if server1:
                server1.stop()
                server1.join()

    def test03_dht_collisions_resolutions(self):
        server = server1 = None
        try:
            home1 = '/tmp/node_1986_home'
            home2 = '/tmp/node_1987_home'
            if os.path.exists(home1):
                shutil.rmtree(home1)
            os.mkdir(home1)
            if os.path.exists(home2):
                shutil.rmtree(home2)
            os.mkdir(home2)

            server = TestServerThread(1986, home1, init_node=True)
            server.start()

            server1 = TestServerThread(1987, home2)
            server1.start()

            time.sleep(.5)
            self.assertNotEqual(server1.operator.status, DS_NORMALWORK)

            dht_range = server.operator.get_dht_range()
            dht_operator.DHT_CYCLE_TRY_COUNT = 10
            split_range_request.ALLOW_FREE_SIZE_PERCENTS = 70

            print 'Starting discovery...'

            packet_obj = FabnetPacketRequest(method='DiscoveryOperation', sender='127.0.0.1:1987')
            rcode, rmsg = server1.operator.call_node('127.0.0.1:1986', packet_obj)
            self.assertEqual(rcode, 0, rmsg)

            time.sleep(1)

            node86_range = server.operator.get_dht_range()
            node87_range = server1.operator.get_dht_range()
            self.assertEqual(node86_range.get_start(), 0L)
            self.assertEqual(node86_range.get_end(), MAX_HASH/2)
            self.assertEqual(node87_range.get_start(), MAX_HASH/2+1)
            self.assertEqual(node87_range.get_end(), MAX_HASH)

            table = server.operator.ranges_table.copy()
            self.assertEqual(len(table), 2)
            self.assertEqual(table[0].start, 0)
            self.assertEqual(table[0].end, MAX_HASH/2)
            self.assertEqual(table[1].start, MAX_HASH/2+1)
            self.assertEqual(table[1].end, MAX_HASH)

            self.assertEqual(server1.operator.status, DS_NORMALWORK)

            print 'REMOVING 1987 NODE RANGE FROM DHT'
            rm_list = [(node87_range.get_start(), node87_range.get_end(), '127.0.0.1:1987')]
            params = {'append': [], 'remove': rm_list}
            packet_obj = FabnetPacketRequest(method='UpdateHashRangeTable', sender='127.0.0.1:1986', parameters=params)
            server.operator.call_network(packet_obj)

            time.sleep(2)

        finally:
            if server:
                server.stop()
                server.join()
            if server1:
                server1.stop()
                server1.join()


if __name__ == '__main__':
    unittest.main()

