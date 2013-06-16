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
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.fri_base import RamBasedBinaryData
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER, ET_INFO, ET_ALERT, RC_PERMISSION_DENIED
from fabnet.dht_mgmt.data_block import DataBlockHeader
from fabnet.dht_mgmt import constants
from fabnet.core.config import Config
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.monitor.constants import MONITOR_DB
from fabnet.core.node import Node
from fabnet.dht_mgmt.hash_ranges_table import HashRangesTable
from fabnet.dht_mgmt.dht_operator import DHTOperator
from fabnet.core.operator import OperatorClient
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

from fabnet.utils.logger import core_logger as logger
from fabnet.dht_mgmt.constants import DS_NORMALWORK, RC_OLD_DATA

#logger.setLevel(logging.DEBUG)

MAX_HASH = constants.MAX_HASH


class TestServerThread(threading.Thread):
    def __init__(self, port, home_dir, neighbour=None, is_monitor=False, config={}):
        threading.Thread.__init__(self)
        self.port = port
        self.home_dir = home_dir
        self.stopped = True
        self.operator = None
        self.neighbour = neighbour
        self.config = config
        self.is_monitor = is_monitor
        self.__lock = threading.Lock()

    def run(self):
        address = '127.0.0.1:%s'%self.port
        if self.is_monitor:
            node_type = 'Monitor'
        else:
            node_type = 'DHT'

        config = {'WAIT_RANGE_TIMEOUT': 0.1,
                 'INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT': 0.1,
                 'MONITOR_DHT_RANGES_TIMEOUT': 1,
                 'CHECK_HASH_TABLE_TIMEOUT': 1,
                 'WAIT_FILE_MD_TIMEDELTA': 0.1,
                 'WAIT_DHT_TABLE_UPDATE': 0.2}
        config.update(self.config)

        node = Node('127.0.0.1', self.port, self.home_dir, 'node_%s'%self.port,
                    ks_path=None, ks_passwd=None, node_type=node_type, config=config)
        node.start(self.neighbour)

        self.__lock.acquire()
        try:
            self.operator = OperatorClient('node_%s'%self.port)
            self.stopped = False
        finally:
            self.__lock.release()

        while not self.stopped:
            time.sleep(0.1)

        node.stop()

    def stop(self):
        self.stopped = True
        self.join()

    def get_stat(self):
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)

        client = FriClient()
        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet_obj)
        if ret_packet.ret_code != 0:
            raise Exception('cant get node statistic. details: %s'%ret_packet.ret_message)
        return ret_packet.ret_parameters

    def put_data_block(self, data, key, is_replica=False, npkey=False, user_id=0):
        checksum = hashlib.sha1(data).hexdigest()
        params = {'key': key, 'checksum': checksum, 'is_replica':is_replica, \
                'carefully_save': True, 'replica_count': 2, 'user_id': user_id}

        if not npkey:
            params['primary_key'] = key

        req = FabnetPacketRequest(method='PutDataBlock',\
                            binary_data=data, sync=True,
                            parameters=params)
        client = FriClient()
        resp = client.call_sync('127.0.0.1:%s'%self.port, req)
        return resp


    def get_data_block(self, key, is_replica=False, user_id=0):
        params = {'key': key, 'is_replica':is_replica}
        req = FabnetPacketRequest(method='GetDataBlock',\
                            sync=True, parameters=params)
        client = FriClient(session_id=user_id)
        resp = client.call_sync('127.0.0.1:%s'%self.port, req)
        if resp.ret_code == constants.RC_NO_DATA:
            return None
        if resp.ret_code != 0:
            raise Exception('GetDataBlock operation failed on %s. Details: %s'%('127.0.0.1:%s'%self.port, resp.ret_message))
        return resp.binary_data

    def put(self, data):
        client = FriClient()
        checksum = hashlib.sha1(data).hexdigest()

        params = {'checksum': checksum, 'wait_writes_count': 3}
        data = RamBasedBinaryData(data, 20)
        packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=data, sync=True)
        #print '========SENDING DATA BLOCK %s (%s chunks)'%(packet_obj, data.chunks_count())

        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet_obj)
        #print '========SENDED DATA BLOCK'
        if ret_packet.ret_code != 0:
            raise Exception('put data failed: %s'%ret_packet.ret_message)
        return ret_packet.ret_parameters['key']

    def get_range_dir(self):
        stat = self.get_stat()
        h_start = stat['DHTInfo']['range_start']
        h_end = stat['DHTInfo']['range_end']
        return os.path.join(self.home_dir, 'dht_range/%s_%s'%(h_start, h_end))

    def get_replicas_dir(self):
        return os.path.join(self.home_dir, 'dht_range/replica_data')

    def get_status(self):
        self.__lock.acquire()
        try:
            if self.operator is None:
                return None
            return self.operator.get_status()
        finally:
            self.__lock.release()


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

            server = TestServerThread(1986, home1)
            server.start()
            time.sleep(1)
            server1 = TestServerThread(1987, home2, neighbour='127.0.0.1:1986')
            server1.start()
            time.sleep(.2)
            self.__wait_oper_status(server1, DS_NORMALWORK)

            node86_stat = server.get_stat()
            node87_stat = server1.get_stat()

            self.assertEqual(long(node86_stat['DHTInfo']['range_start'], 16), 0L)
            self.assertEqual(long(node86_stat['DHTInfo']['range_end'], 16), MAX_HASH/2)
            self.assertEqual(long(node87_stat['DHTInfo']['range_start'], 16), MAX_HASH/2+1)
            self.assertEqual(long(node87_stat['DHTInfo']['range_end'], 16), MAX_HASH)

            table_dump = server.operator.dump_ranges_table()
            table = HashRangesTable()
            table.load(table_dump)
            self.assertEqual(table.count(), 2)
            hr = table.find(0)
            self.assertEqual(hr.start, 0)
            self.assertEqual(hr.end, MAX_HASH/2)
            hr = table.find(MAX_HASH)
            self.assertEqual(hr.start, MAX_HASH/2+1)
            self.assertEqual(hr.end, MAX_HASH)

            data_block = 'Hello, fabregas!'
            replica_block = 'This is replica data!'
            resp = server.put_data_block(data_block, MAX_HASH-100500) #should be appended into reservation range
            self.assertEqual(resp.ret_code, RC_OK, resp.ret_message)
            resp = server1.put_data_block(replica_block, 100, is_replica=True)
            self.assertEqual(resp.ret_code, RC_OK, resp.ret_message)

            time.sleep(2)

            data = server1.get_data_block(MAX_HASH-100500)
            self.assertEqual(data.data(), data_block)
            data = server.get_data_block(100, is_replica=True)
            self.assertEqual(data.data(), replica_block)

            data = server.get_data_block(MAX_HASH-100500)
            self.assertEqual(data, None)
            data = server1.get_data_block(100, is_replica=True)
            self.assertEqual(data, None)

            resp = server1.put_data_block('New data block', MAX_HASH-100500, user_id=3232)
            self.assertEqual(resp.ret_code, RC_PERMISSION_DENIED, resp.ret_message)
            data = server1.get_data_block(MAX_HASH-100500)
            self.assertEqual(data.data(), data_block)

            resp = server1.put_data_block('New data block', MAX_HASH-100, user_id=3232)
            self.assertEqual(resp.ret_code, RC_OK, resp.ret_message)
            data = server1.get_data_block(MAX_HASH-100, user_id=3232)
            self.assertEqual(data.data(), 'New data block')
            try:
                server1.get_data_block(MAX_HASH-100)
            except Exception, err:
                pass
            else:
                raise Esception('should be exception in this case')
        finally:
            if server:
                server.stop()
            if server1:
                server1.stop()

    def __wait_oper_status(self, server, status):
        for i in xrange(10):
            if server.get_status() == status:
                return
            time.sleep(.5)
        self.assertEqual(server.get_status(), status)

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

            server = TestServerThread(1986, home1)
            server.start()
            time.sleep(1)
            server1 = TestServerThread(1987, home2, neighbour='127.0.0.1:1986',  config={'DHT_CYCLE_TRY_COUNT':10, 'ALLOW_USED_SIZE_PERCENTS':0})
            server1.start()
            time.sleep(1.5)
            self.assertNotEqual(server1.operator.get_status(), DS_NORMALWORK)
            for i in xrange(3):
                try:
                    server.operator.split_range(0, 100500)
                    break
                except Exception, err:
                    time.sleep(.1)
            time.sleep(.2)
            server.operator.join_subranges()
            time.sleep(.2)
            server1.operator.update_config({'ALLOW_USED_SIZE_PERCENTS':70})
            self.__wait_oper_status(server1, DS_NORMALWORK)

            data_block = 'Hello, fabregas! '*100
            checksum = hashlib.sha1(data_block).hexdigest()
            header1 = DataBlockHeader.pack('0000000000000000000000000000000000000000', 2, checksum)
            self.assertEqual(len(header1), DataBlockHeader.HEADER_LEN)
            time.sleep(1)

            server1.put_data_block(data_block, MAX_HASH/2+100)
            resp = server1.put_data_block(header1+data_block, MAX_HASH/2+100, npkey=True)
            self.assertEqual(resp.ret_code, RC_OLD_DATA, resp.ret_message)

            data = server1.get_data_block(MAX_HASH/2+100)
            self.assertEqual(data.data(), data_block)
        finally:
            if server:
                server.stop()
            if server1:
                server1.stop()


    def __test03_dht_collisions_resolutions(self):
        server = server1 = None
        try:
            home1 =  self._make_fake_hdd('node_1986', 1024, '/dev/loop0')
            home2 =  self._make_fake_hdd('node_1987', 1024, '/dev/loop1')

            server = TestServerThread(1986, home1)
            server.start()
            time.sleep(1)
            server1 = TestServerThread(1987, home2, neighbour='127.0.0.1:1986',  config={'DHT_CYCLE_TRY_COUNT':10, 'ALLOW_USED_SIZE_PERCENTS':70})
            server1.start()
            self.__wait_oper_status(server1, DS_NORMALWORK)

            print 'REMOVING 1987 NODE RANGE FROM DHT'
            rm_list = [(MAX_HASH/2+1, MAX_HASH, '127.0.0.1:1987')]
            params = {'append': [], 'remove': rm_list}
            packet_obj = FabnetPacketRequest(method='UpdateHashRangeTable', sender='127.0.0.1:1986', parameters=params)
            server.operator.call_network(packet_obj)

            self.__wait_oper_status(server, DS_NORMALWORK)
            self.__wait_oper_status(server1, DS_NORMALWORK)

            table_dump = server1.operator.dump_ranges_table()
            table = HashRangesTable()
            table.load(table_dump)
            self.assertEqual(table.count(), 2)
            hr = table.find(0)
            self.assertEqual(hr.start, 0)
            self.assertEqual(hr.end, MAX_HASH/2)
            hr = table.find(MAX_HASH)
            self.assertEqual(hr.start, MAX_HASH/2+1)
            self.assertEqual(hr.end, MAX_HASH)
        finally:
            if server:
                server.stop()
            if server1:
                server1.stop()
            self._destroy_fake_hdd('node_1986', '/dev/loop0')
            self._destroy_fake_hdd('node_1987', '/dev/loop1')


    def test04_dht_pull_subrange(self):
        server = server1 = None
        try:
            home1 =  self._make_fake_hdd('node_1986', 1024, '/dev/loop0')
            home2 =  self._make_fake_hdd('node_1987', 1024, '/dev/loop1')

            server = TestServerThread(1986, home1)
            server.start()
            time.sleep(1)
            server1 = TestServerThread(1987, home2, neighbour='127.0.0.1:1986')
            server1.start()
            self.__wait_oper_status(server, DS_NORMALWORK)
            self.__wait_oper_status(server1, DS_NORMALWORK)

            step = MAX_HASH/2/100
            for i in range(100):
                data = ''.join(random.choice(string.letters) for i in xrange(7*1024))
                server.put_data_block(data, i*step)

            node86_stat = server.get_stat()
            self.assertEqual(node86_stat['DHTInfo']['free_size_percents'] < 20, True)
            time.sleep(1)

            step = MAX_HASH/2/10
            for i in range(10):
                data = ''.join(random.choice(string.letters) for i in xrange(8*1024))
                server.put_data_block(data, i*step, is_replica=True)

            node86_stat = server.get_stat()
            node87_stat = server1.get_stat()

            self.assertEqual(node86_stat['DHTInfo']['free_size_percents'] < 10, True, node86_stat['DHTInfo']['free_size_percents'] )
            self.assertEqual(node87_stat['DHTInfo']['free_size_percents'] > 90, True)
            time.sleep(1.5)
            node86_stat = server.get_stat()
            node87_stat = server1.get_stat()
            self.assertEqual(node86_stat['DHTInfo']['free_size_percents'] > 15, True, node86_stat['DHTInfo']['free_size_percents'] )
            self.assertEqual(node87_stat['DHTInfo']['free_size_percents'] < 90, True)
        finally:
            if server:
                server.stop()
            if server1:
                server1.stop()
            time.sleep(2)
            self._destroy_fake_hdd('node_1986', '/dev/loop0')
            self._destroy_fake_hdd('node_1987', '/dev/loop1')


    def test05_repair_data(self):
        server = server1 = None
        try:
            home1 = '/tmp/node_1986_home'
            home2 = '/tmp/node_1987_home'
            monitor_home = '/tmp/node_monitor_home'
            if os.path.exists(home1):
                shutil.rmtree(home1)
            os.mkdir(home1)
            if os.path.exists(home2):
                shutil.rmtree(home2)
            os.mkdir(home2)
            if os.path.exists(monitor_home):
                shutil.rmtree(monitor_home)
            os.mkdir(monitor_home)

            server = TestServerThread(1986, home1)
            server.start()
            time.sleep(1)
            server1 = TestServerThread(1987, home2, neighbour='127.0.0.1:1986')
            server1.start()
            time.sleep(1)
            monitor = TestServerThread(1990, monitor_home, is_monitor=True, neighbour='127.0.0.1:1986')
            monitor.start()
            time.sleep(1.5)

            self.__wait_oper_status(server, DS_NORMALWORK)
            self.__wait_oper_status(server1, DS_NORMALWORK)

            data = 'Hello, fabregas!'*10
            data_key = server.put(data)

            data = 'This is replica data!'*10
            data_key2 = server1.put(data)

            conn = DBConnection("dbname=%s user=postgres"%MONITOR_DB)
            conn.execute('DELETE FROM notification')

            time.sleep(.2)
            client = FriClient()
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters={})
            rcode, rmsg = client.call('127.0.0.1:1986', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(1.5)

            events = conn.select('SELECT notify_type, node_address, notify_msg FROM notification')
            conn.execute('DELETE FROM notification')

            stat = 'processed_local_blocks=%i, invalid_local_blocks=0, repaired_foreign_blocks=0, failed_repair_foreign_blocks=0'
            self.assertEqual(len(events), 2)
            event86 = event87 = None
            for event in events:
                if event[1] == '127.0.0.1:1986':
                    event86 = event
                elif event[1] == '127.0.0.1:1987':
                    event87 = event

            self.assertEqual(event86[0], ET_INFO)
            self.assertEqual(event86[1], '127.0.0.1:1986', events)
            cnt86 = len(os.listdir(server.get_range_dir())) + len(os.listdir(server.get_replicas_dir()))
            self.assertTrue(stat%cnt86 in event86[2], event86[2])

            self.assertEqual(event87[0], ET_INFO)
            self.assertEqual(event87[1], '127.0.0.1:1987')
            cnt87 = len(os.listdir(server1.get_range_dir())) + len(os.listdir(server1.get_replicas_dir()))
            self.assertTrue(stat%cnt87 in event87[2], event87[2])

            node86_stat = server.get_stat()
            server.stop()
            server.join()
            server = None
            time.sleep(1)

            params = {'check_range_start': node86_stat['DHTInfo']['range_start'], 'check_range_end': node86_stat['DHTInfo']['range_end']}
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters=params)
            rcode, rmsg = client.call('127.0.0.1:1987', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(2)

            events = conn.select("SELECT notify_type, node_address, notify_msg FROM notification WHERE notify_topic='RepairDataBlocks'")
            conn.execute('DELETE FROM notification')

            self.assertEqual(len(events), 1, events)
            event86 = event87 = None
            for event in events:
                if event[1] == '127.0.0.1:1986':
                    event86 = event
                elif event[1] == '127.0.0.1:1987':
                    event87 = event
            self.assertEqual(event87[0], ET_INFO)
            self.assertEqual(event87[1], '127.0.0.1:1987')
            stat_rep = 'processed_local_blocks=%i, invalid_local_blocks=0, repaired_foreign_blocks=%i, failed_repair_foreign_blocks=0'
            self.assertTrue(stat_rep%(cnt87, cnt86) in event87[2], event87[2])

            open(os.path.join(server1.get_range_dir(), data_key), 'wr').write('wrong data')
            open(os.path.join(server1.get_range_dir(), data_key2), 'ar').write('wrong data')

            time.sleep(.2)
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters={})
            rcode, rmsg = client.call('127.0.0.1:1987', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(2)

            events = conn.select('SELECT notify_type, node_address, notify_msg FROM notification')
            conn.execute('DELETE FROM notification')

            self.assertEqual(len(events), 1)
            for event in events:
                if event[1] == '127.0.0.1:1986':
                    event86 = event
                elif event[1] == '127.0.0.1:1987':
                    event87 = event

            self.assertEqual(event87[1], '127.0.0.1:1987')
            stat_rep = 'processed_local_blocks=%i, invalid_local_blocks=%i, repaired_foreign_blocks=%i, failed_repair_foreign_blocks=0'
            self.assertTrue(stat_rep%(cnt87+cnt86, 1, 2) in event87[2], event87[2])
            conn.close()
        finally:
            if server:
                server.stop()
            if server1:
                server1.stop()
            if monitor:
                monitor.stop()

    def _make_fake_hdd(self, name, size, dev='/dev/loop0'):
        os.system('sudo rm -rf /tmp/mnt_%s'%name)
        os.system('sudo rm -rf /tmp/%s'%name)
        os.system('dd if=/dev/zero of=/tmp/%s bs=1024 count=%s'%(name, size))
        os.system('sudo umount /tmp/mnt_%s'%name)
        os.system('sudo losetup -d %s'%dev)
        os.system('sudo losetup %s /tmp/%s'%(dev, name))
        os.system('sudo mkfs -t ext2 -m 1 -v %s'%dev)
        os.system('sudo mkdir /tmp/mnt_%s'%name)
        os.system('sudo mount -t ext2 %s /tmp/mnt_%s'%(dev, name))
        os.system('sudo chmod 777 /tmp/mnt_%s -R'%name)
        os.system('rm -rf /tmp/mnt_%s/*'%name)
        return '/tmp/mnt_%s'%name

    def _destroy_fake_hdd(self, name, dev='/dev/loop0'):
        ret = os.system('sudo umount /tmp/mnt_%s'%name)
        self.assertEqual(ret, 0, 'destroy_fake_hdd failed')
        ret = os.system('sudo losetup -d %s'%dev)
        self.assertEqual(ret, 0, 'destroy_fake_hdd failed')
        os.system('sudo rm /tmp/%s'%name)
        os.system('sudo rm -rf /tmp/mnt_%s'%name)

if __name__ == '__main__':
    unittest.main()

