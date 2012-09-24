import unittest
import time
import os
import logging
import shutil
import threading
import json
import random
from fabnet.core.fri_base import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.dht_mgmt import constants
constants.WAIT_RANGE_TIMEOUT = 0.1
constants.INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT = 0.1
constants.WAIT_RANGES_TIMEOUT = 0.3
from fabnet.dht_mgmt.dht_operator import DHTOperator
from fabnet.dht_mgmt import dht_operator
from fabnet.dht_mgmt.operations import split_range_request
from fabnet.dht_mgmt.operations.get_range_data_request import GetRangeDataRequestOperation
from fabnet.dht_mgmt.operations.get_ranges_table import GetRangesTableOperation
from fabnet.dht_mgmt.operations.put_data_block import PutDataBlockOperation
from fabnet.dht_mgmt.operations.split_range_cancel import SplitRangeCancelOperation
from fabnet.dht_mgmt.operations.split_range_request import SplitRangeRequestOperation
from fabnet.dht_mgmt.operations.update_hash_range_table import UpdateHashRangeTableOperation

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
        operator = DHTOperator(address, self.home_dir, is_init_node=self.init_node)
        self.operator = operator

        operator.register_operation('GetRangeDataRequest', GetRangeDataRequestOperation)
        operator.register_operation('GetRangesTable', GetRangesTableOperation)
        operator.register_operation('PutDataBlock', PutDataBlockOperation)
        operator.register_operation('SplitRangeCancel', SplitRangeCancelOperation)
        operator.register_operation('SplitRangeRequest', SplitRangeRequestOperation)
        operator.register_operation('UpdateHashRangeTable', UpdateHashRangeTableOperation)
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

            server1.operator.set_neighbour(2, 'fake_node_addr:8080')
            time.sleep(.1)
            server1.operator.set_neighbour(1, '127.0.0.1:1986')
            time.sleep(1)

            node86_range = server.operator.get_dht_range()
            node87_range = server1.operator.get_dht_range()
            self.assertEqual(node86_range.get_start(), 0L)
            self.assertEqual(node86_range.get_end(), MAX_HASH/2-1)
            self.assertEqual(node87_range.get_start(), MAX_HASH/2)
            self.assertEqual(node87_range.get_end(), MAX_HASH)

            table = server.operator.ranges_table.copy()
            self.assertEqual(len(table), 2)
            self.assertEqual(table[0].start, 0)
            self.assertEqual(table[0].end, MAX_HASH/2-1)
            self.assertEqual(table[1].start, MAX_HASH/2)
            self.assertEqual(table[1].end, MAX_HASH)

            self.assertEqual(server1.operator.status, DS_NORMALWORK)
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


            server1.operator.set_neighbour(2, '127.0.0.1:1986')
            time.sleep(.2)
            dht_range.join_subranges()

            time.sleep(.2)
            split_range_request.ALLOW_FREE_SIZE_PERCENTS = 70


            time.sleep(1)

            node86_range = server.operator.get_dht_range()
            node87_range = server1.operator.get_dht_range()
            self.assertEqual(node86_range.get_start(), 0L)
            self.assertEqual(node86_range.get_end(), MAX_HASH/2-1)
            self.assertEqual(node87_range.get_start(), MAX_HASH/2)
            self.assertEqual(node87_range.get_end(), MAX_HASH)

            table = server.operator.ranges_table.copy()
            self.assertEqual(len(table), 2)
            self.assertEqual(table[0].start, 0)
            self.assertEqual(table[0].end, MAX_HASH/2-1)
            self.assertEqual(table[1].start, MAX_HASH/2)
            self.assertEqual(table[1].end, MAX_HASH)

            self.assertEqual(server1.operator.status, DS_NORMALWORK)
        finally:
            if server:
                server.stop()
                server.join()
            if server1:
                server1.stop()
                server1.join()

if __name__ == '__main__':
    unittest.main()

