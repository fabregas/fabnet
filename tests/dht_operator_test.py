import unittest
import threading
import time
import os
import logging
import json
import shutil
from datetime import datetime
from fabnet.utils.logger import logger
from fabnet.dht_mgmt.dht_operator import DHTOperator
from fabnet.dht_mgmt import dht_operator
from fabnet.core.fri_base import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.dht_mgmt.operations.check_hash_range_table import CheckHashRangeTableOperation
from fabnet.dht_mgmt.constants import RC_NEED_UPDATE

dht_operator.WAIT_RANGE_TIMEOUT = 0.5
dht_operator.INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT = 0.1
dht_operator.MONITOR_DHT_RANGES_TIMEOUT = 1
dht_operator.WAIT_RANGES_TIMEOUT = 0.3
dht_operator.RESERV_RANGE_FILE_MD_TIMEDELTA = 0.1

logger.setLevel(logging.DEBUG)

TEST_FS_RANGE_DIR = '/tmp/test_fs_ranges'


class TestFSMappedRanges(unittest.TestCase):
    def test00_init(self):
        if os.path.exists(TEST_FS_RANGE_DIR):
            shutil.rmtree(TEST_FS_RANGE_DIR)
        os.mkdir(TEST_FS_RANGE_DIR)

    def test99_destroy(self):
        if os.path.exists(TEST_FS_RANGE_DIR):
            shutil.rmtree(TEST_FS_RANGE_DIR)

    def test02_main(self):
        call_stack = []
        def call_node_simulator(nodeaddr, request):
            call_stack.append((nodeaddr, request.parameters['start_key'], request.parameters['end_key']))
            return 0, 'ok'

        operator = DHTOperator('127.0.0.1:1987', home_dir=TEST_FS_RANGE_DIR)

        operator.ranges_table.append(0, 99, 'first_range_holder')
        operator.ranges_table.append(100, 149, 'second_range_holder')
        operator.ranges_table.append(300, 499, 'third_range_holder')

        operator.call_node = call_node_simulator
        operator.start_as_dht_member()
        operator.start_as_dht_member()
        operator.start_as_dht_member()

        self.assertEqual(len(call_stack), 3)
        self.assertEqual(call_stack[0][0], 'third_range_holder')
        self.assertEqual(call_stack[0][1], 400)
        self.assertEqual(call_stack[0][2], 499)

        self.assertEqual(call_stack[1][0], 'first_range_holder')
        self.assertEqual(call_stack[1][1], 50)
        self.assertEqual(call_stack[1][2], 99)

        self.assertEqual(call_stack[2][0], 'second_range_holder')
        self.assertEqual(call_stack[2][1], 125)
        self.assertEqual(call_stack[2][2], 149)

        operator.start_as_dht_member() #wait timeout
        self.assertEqual(call_stack[3][0], 'second_range_holder')
        self.assertEqual(call_stack[3][1], 125)
        self.assertEqual(call_stack[3][2], 149)
        operator.stop()

    def test03_discovery_range(self):
        shutil.rmtree(TEST_FS_RANGE_DIR)
        os.mkdir(TEST_FS_RANGE_DIR)
        os.makedirs(os.path.join(TEST_FS_RANGE_DIR, 'dht_range', '%040x_%040x'%(0, 99)))

        call_stack = []
        def call_node_simulator(nodeaddr, request):
            call_stack.append((nodeaddr, request.parameters['start_key'], request.parameters['end_key']))
            return 0, 'ok'

        operator = DHTOperator('127.0.0.1:1987', home_dir=TEST_FS_RANGE_DIR)

        try:
            operator.ranges_table.append(0, 99, 'first_range_holder')
            checksum, last_dm = operator.ranges_table.get_checksum()
            operator.ranges_table.append(100, 149, 'second_range_holder')
            operator.ranges_table.append(300, 499, 'third_range_holder')
            checksum1, last_dm1 = operator.ranges_table.get_checksum()
            self.assertNotEqual(checksum, checksum1)
            self.assertNotEqual(last_dm, last_dm1)

            tbl_dump = operator.ranges_table.dump()
            operator.ranges_table.load(tbl_dump)
            checksum2, last_dm2 = operator.ranges_table.get_checksum()
            self.assertEqual(last_dm2, last_dm1)
            self.assertEqual(checksum2, checksum1)

            operator.call_node = call_node_simulator
            operator.start_as_dht_member()
            operator.start_as_dht_member()
            operator.start_as_dht_member()

            self.assertEqual(len(call_stack), 3)
            self.assertEqual(call_stack[0][0], 'first_range_holder')
            self.assertEqual(call_stack[0][1], 50)
            self.assertEqual(call_stack[0][2], 99)

            self.assertEqual(call_stack[1][0], 'third_range_holder')
            self.assertEqual(call_stack[1][1], 400)
            self.assertEqual(call_stack[1][2], 499)

            self.assertEqual(call_stack[2][0], 'second_range_holder')
            self.assertEqual(call_stack[2][1], 125)
            self.assertEqual(call_stack[2][2], 149)

            def call_node_error_simulator(nodeaddr, request):
                if nodeaddr == 'second_range_holder':
                    return 666, 'No route to host!'

                call_stack.append((nodeaddr, request.parameters['start_key'], request.parameters['end_key']))
                return 0, 'ok'
            operator.call_node = call_node_error_simulator
            operator.start_as_dht_member()
            self.assertEqual(call_stack[3][0], 'third_range_holder')
            self.assertEqual(call_stack[3][1], 400)
            self.assertEqual(call_stack[3][2], 499)
        finally:
            operator.stop()

    def test04_check_hash_table(self):
        call_stack = []
        def call_node_simulator(nodeaddr, request, sync):
            call_stack.append((nodeaddr, sync, request))
            return 0, 'ok'

        try:
            operator = DHTOperator('127.0.0.1:1987', home_dir=TEST_FS_RANGE_DIR)

            operator.ranges_table.append(0, 99, 'first_range_holder')
            operator.ranges_table.append(100, 149, 'second_range_holder')
            operator.ranges_table.append(300, 499, 'third_range_holder')

            operator.call_node = call_node_simulator
            operator.register_operation('CheckHashRangeTable', CheckHashRangeTableOperation)

            operator1 = DHTOperator('127.0.0.1:1986', home_dir=TEST_FS_RANGE_DIR)

            operator1.ranges_table.append(0, 99, 'first_range_holder')
            operator1.ranges_table.append(100, 149, 'second_range_holder')
            operator1.ranges_table.append(300, 499, 'third_range_holder')

            operator1.call_node = call_node_simulator
            operator1.register_operation('CheckHashRangeTable', CheckHashRangeTableOperation)

            csum, ldm = operator.ranges_table.get_checksum()
            packet = FabnetPacketRequest(method='CheckHashRangeTable', sender=operator.self_address, parameters={'checksum': csum, 'last_dm': ldm.isoformat()})
            resp = operator1.process(packet)
            self.assertEqual(resp.ret_code, 0)

            operator1.ranges_table.append(500, 599, 'forth_range_holder')

            packet = FabnetPacketRequest(method='CheckHashRangeTable', sender=operator.self_address, parameters={'checksum': csum, 'last_dm': ldm.isoformat()})
            resp = operator1.process(packet)
            self.assertEqual(resp.ret_code, RC_NEED_UPDATE)
            callback_resp = resp

            csum, ldm = operator1.ranges_table.get_checksum()
            packet = FabnetPacketRequest(message_id=packet.message_id, method='CheckHashRangeTable', \
                                    sender=operator1.self_address, parameters={'checksum': csum, 'last_dm': ldm.isoformat()})
            resp = operator.process(packet)
            self.assertEqual(resp.ret_code, 0, resp.ret_message)

            operator.callback(callback_resp)
            self.assertEqual(len(call_stack), 2)
            self.assertEqual(call_stack[0][0], call_stack[1][0])
            self.assertEqual(call_stack[0][0], '127.0.0.1:1986')
            self.assertEqual(call_stack[0][2].method, 'GetRangesTable')
            self.assertEqual(call_stack[1][2].method, 'GetRangesTable')

            print 'GENERATE EXCEPTION'
            packet = FabnetPacketRequest(method='CheckHashRangeTable', sender=operator.self_address)
            resp = operator.process(packet)
            self.assertNotEqual(resp.ret_code, 0)
            self.assertNotEqual(resp.ret_code, RC_NEED_UPDATE)
        finally:
            operator.stop()
            operator1.stop()




if __name__ == '__main__':
    unittest.main()

