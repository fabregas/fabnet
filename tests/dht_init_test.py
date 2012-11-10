import unittest
import time
import os
import logging
import shutil
import threading
import json
import random
import string
import hashlib
from fabnet.core.fri_server import FriServer, FriClient, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER, ET_INFO, ET_ALERT
from fabnet.dht_mgmt.data_block import DataBlock
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
from fabnet.dht_mgmt.constants import DS_NORMALWORK, RC_OLD_DATA

logger.setLevel(logging.DEBUG)

MAX_HASH = constants.MAX_HASH


class MonitoredDHTOperator(DHTOperator):
    def __init__(self, self_address, home_dir='/tmp/', certfile=None, is_init_node=False, node_name='unknown'):
        DHTOperator.__init__(self, self_address, home_dir, certfile, is_init_node, node_name)
        self.events = []

    def on_network_notify(self, notify_type, notify_provider, message):
        DHTOperator.on_network_notify(self, notify_type, notify_provider, message)
        self.events.append((notify_type, notify_provider, message))


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
        operator = MonitoredDHTOperator(address, self.home_dir, is_init_node=self.init_node, node_name=self.port)
        self.operator = operator

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

            data = 'Hello, fabregas!'
            checksum = hashlib.sha1(data).hexdigest()
            data_block = DataBlock(data, checksum)
            data_block.validate()
            resr_data, checksum = data_block.pack('0000000000000000000000000000000000000000', 2)
            node86_range.put(MAX_HASH-100500, resr_data) #should be appended into reservation range

            data = 'This is replica data!'
            checksum = hashlib.sha1(data).hexdigest()
            data_block = DataBlock(data, checksum)
            data_block.validate()
            repl_data, checksum = data_block.pack('0000000000000000000000000000000000000000', 2)
            node87_range.put_replica(100, repl_data)

            time.sleep(1.5)
            data = node87_range.get(MAX_HASH-100500)
            self.assertEqual(data, resr_data)
            data = node86_range.get_replica(100)
            self.assertEqual(data, repl_data)
            try:
                node87_range.get_replica(100)
            except Exception, err:
                pass
            else:
                raise Exception('should be exception in this case')

            try:
                node86_range.get(MAX_HASH-100500)
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
            split_range_request.ALLOW_USED_SIZE_PERCENTS = 0

            data = 'Hello, fabregas! '*100
            checksum = hashlib.sha1(data).hexdigest()
            data_block = DataBlock(data, checksum)
            data_block.validate()
            data2, checksum2 = data_block.pack('0000000000000000000000000000000000000000', 2)
            _, _, _, stored_dt = DataBlock.read_header(data2)
            time.sleep(1)
            checksum = hashlib.sha1(data).hexdigest()
            data_block = DataBlock(data, checksum)
            data_block.validate()
            data, checksum = data_block.pack('0000000000000000000000000000000000000000', 2)
            _, _, _, stored_dt = DataBlock.read_header(data)

            params = {'key': MAX_HASH/2+100, 'checksum': checksum, 'carefully_save': True}
            packet_obj = FabnetPacketRequest(method='PutDataBlock', parameters=params, binary_data=data, sync=True)
            fri_client = FriClient()
            resp = fri_client.call_sync('127.0.0.1:1987', packet_obj)
            self.assertEqual(resp.ret_code, 0)
            f_path = os.path.join(home2, 'dht_range/0000000000000000000000000000000000000000_ffffffffffffffffffffffffffffffffffffffff/8000000000000000000000000000000000000063')
            self.assertTrue(os.path.exists(f_path))

            params = {'key': MAX_HASH/2+100, 'checksum': checksum2, 'carefully_save': True}
            packet_obj = FabnetPacketRequest(method='PutDataBlock', parameters=params, binary_data=data2, sync=True)
            resp = fri_client.call_sync('127.0.0.1:1987', packet_obj)
            self.assertEqual(resp.ret_code, RC_OLD_DATA, resp.ret_message)


            dht_range.split_range(0, 100500)
            server1.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:1986')
            time.sleep(.2)
            dht_range.join_subranges()
            time.sleep(.2)

            split_range_request.ALLOW_USED_SIZE_PERCENTS = 70
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
            home1 =  self._make_fake_hdd('node_1986', 1024, '/dev/loop0')
            home2 =  self._make_fake_hdd('node_1987', 1024, '/dev/loop1')

            server = TestServerThread(1986, home1, init_node=True)
            server.start()

            server1 = TestServerThread(1987, home2)
            server1.start()

            time.sleep(.5)
            self.assertNotEqual(server1.operator.status, DS_NORMALWORK)

            dht_range = server.operator.get_dht_range()
            dht_operator.DHT_CYCLE_TRY_COUNT = 10
            split_range_request.ALLOW_USED_SIZE_PERCENTS = 70

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
            self._destroy_fake_hdd('node_1986', '/dev/loop0')
            self._destroy_fake_hdd('node_1987', '/dev/loop1')

    def test04_dht_pull_subrange(self):
        server = server1 = None
        try:
            home1 =  self._make_fake_hdd('node_1986', 1024, '/dev/loop0')
            home2 =  self._make_fake_hdd('node_1987', 1024, '/dev/loop1')

            server = TestServerThread(1986, home1, init_node=True)
            server.start()

            server1 = TestServerThread(1987, home2)
            server1.start()

            time.sleep(.5)
            self.assertNotEqual(server1.operator.status, DS_NORMALWORK)

            server1.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:1986')
            server.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:1987')
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

            step = MAX_HASH/2/100
            for i in range(100):
                data = ''.join(random.choice(string.letters) for i in xrange(7*1024))
                checksum = hashlib.sha1(data).hexdigest()
                data_block = DataBlock(data, checksum)
                data_block.validate()
                resr_data, checksum = data_block.pack('0000000000000000000000000000000000000000', 2)
                node86_range.put(step*i, resr_data)

            self.assertEqual(node86_range.get_free_size_percents() < 20, True)
            time.sleep(1)

            step = MAX_HASH/2/10
            for i in range(10):
                data = ''.join(random.choice(string.letters) for i in xrange(7*1024))
                checksum = hashlib.sha1(data).hexdigest()
                data_block = DataBlock(data, checksum)
                data_block.validate()
                resr_data, checksum = data_block.pack('0000000000000000000000000000000000000000', 2)
                node86_range.put_replica(step*i, resr_data)
            self.assertEqual(node86_range.get_free_size_percents() < 10, True)
            self.assertEqual(node87_range.get_free_size_percents() > 90, True)
            time.sleep(1.5)
            node86_range = server.operator.get_dht_range()
            node87_range = server1.operator.get_dht_range()
            self.assertEqual(node86_range.get_free_size_percents() > 15, True)
            self.assertEqual(node87_range.get_free_size_percents() < 90, True)

            step = MAX_HASH/2/100
            for i in range(95):
                data = ''.join(random.choice(string.letters) for i in xrange(7*1024))
                checksum = hashlib.sha1(data).hexdigest()
                data_block = DataBlock(data, checksum)
                data_block.validate()
                resr_data, checksum = data_block.pack('0000000000000000000000000000000000000000', 2)
                node87_range.put(MAX_HASH/2+step*i, resr_data)
            time.sleep(1)
            node86_range = server.operator.get_dht_range()
            node87_range = server1.operator.get_dht_range()
            self.assertEqual(node86_range.get_free_size_percents() > 15, True)
            self.assertEqual(node87_range.get_free_size_percents() < 10, True)
        finally:
            if server:
                server.stop()
                server.join()
            if server1:
                server1.stop()
                server1.join()
            self._destroy_fake_hdd('node_1986', '/dev/loop0')
            self._destroy_fake_hdd('node_1987', '/dev/loop1')

    def test05_repair_data(self):
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

            server1.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:1986')
            server.operator.set_neighbour(NT_SUPERIOR, '127.0.0.1:1987')
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

            data = 'Hello, fabregas!'
            data_key = self._send_data_block('127.0.0.1:1986', data)

            data = 'This is replica data!'
            data_key2 = self._send_data_block('127.0.0.1:1987', data)

            time.sleep(.2)
            client = FriClient()
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', parameters={})
            rcode, rmsg = client.call('127.0.0.1:1986', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(1.5)

            stat = 'processed_local_blocks=%i, invalid_local_blocks=0, repaired_foreign_blocks=0, failed_repair_foreign_blocks=0'
            self.assertEqual(len(server.operator.events), 2)
            event86 = server.operator.events[0]
            self.assertEqual(event86[0], ET_INFO)
            self.assertEqual(event86[1], '127.0.0.1:1986')
            cnt86 = len(os.listdir(node86_range.get_range_dir())) + len(os.listdir(node86_range.get_replicas_dir()))
            self.assertTrue(stat%cnt86 in event86[2])

            event87 = server.operator.events[1]
            self.assertEqual(event87[0], ET_INFO)
            self.assertEqual(event87[1], '127.0.0.1:1987')
            cnt87 = len(os.listdir(node87_range.get_range_dir())) + len(os.listdir(node87_range.get_replicas_dir()))
            self.assertTrue(stat%cnt87 in event87[2])

            server.stop()
            server.join()
            server = None
            time.sleep(1)
            server1.operator.events = []
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', parameters={})
            rcode, rmsg = client.call('127.0.0.1:1987', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(1.5)
            self.assertEqual(len(server1.operator.events), 1)
            event87 = server1.operator.events[0]
            self.assertEqual(event87[0], ET_INFO)
            self.assertEqual(event87[1], '127.0.0.1:1987')
            stat_rep = 'processed_local_blocks=%i, invalid_local_blocks=0, repaired_foreign_blocks=%i, failed_repair_foreign_blocks=0'
            self.assertTrue(stat_rep%(cnt87, cnt86) in event87[2])

            node87_range = server1.operator.get_dht_range()
            open(os.path.join(node87_range.get_range_dir(), data_key), 'wr').write('wrong data')
            open(os.path.join(node87_range.get_range_dir(), data_key2), 'ar').write('wrong data')
            server1.operator.events = []

            time.sleep(.2)
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', parameters={})
            rcode, rmsg = client.call('127.0.0.1:1987', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(1.5)
            self.assertEqual(len(server1.operator.events), 1)
            event87 = server1.operator.events[0]
            self.assertEqual(event87[0], ET_INFO)
            self.assertEqual(event87[1], '127.0.0.1:1987')
            stat_rep = 'processed_local_blocks=%i, invalid_local_blocks=%i, repaired_foreign_blocks=%i, failed_repair_foreign_blocks=0'
            self.assertTrue(stat_rep%(cnt87+cnt86, 1, 2) in event87[2], event87[2])
        finally:
            if server:
                server.stop()
                server.join()
            if server1:
                server1.stop()
                server1.join()

    def _send_data_block(self, address, data):
        client = FriClient()
        checksum = hashlib.sha1(data).hexdigest()

        params = {'checksum': checksum, 'wait_writes_count': 3}
        packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=data, sync=True)

        ret_packet = client.call_sync(address, packet_obj)
        self.assertEqual(ret_packet.ret_code, 0, ret_packet.ret_message)
        self.assertEqual(len(ret_packet.ret_parameters.get('key', '')), 40)
        return ret_packet.ret_parameters['key']

    def _make_fake_hdd(self, name, size, dev='/dev/loop0'):
        os.system('dd if=/dev/zero of=/tmp/%s bs=1024 count=%s'%(name, size))
        os.system('sudo umount /tmp/mnt_%s'%name)
        os.system('sudo losetup -d %s'%dev)
        os.system('sudo losetup %s /tmp/%s'%(dev, name))
        os.system('sudo mkfs -t ext2 -m 1 -v %s'%dev)
        os.system('sudo mkdir /tmp/mnt_%s'%name)
        os.system('sudo mount -t ext2 %s /tmp/mnt_%s'%(dev, name))
        os.system('sudo chmod 777 /tmp/mnt_%s -R'%name)
        return '/tmp/mnt_%s'%name

    def _destroy_fake_hdd(self, name, dev='/dev/loop0'):
        os.system('sudo umount /tmp/mnt_%s'%name)
        os.system('sudo losetup -d %s'%dev)
        os.system('sudo rm /tmp/%s'%name)

if __name__ == '__main__':
    unittest.main()

