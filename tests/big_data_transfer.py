import unittest
import time
import os
import logging
import json
from fabnet.core import constants
constants.CHECK_NEIGHBOURS_TIMEOUT = 1
from fabnet.core.fri_server import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.operator import Operator
from fabnet.core.operation_base import OperationBase
from fabnet.utils.logger import logger
from datetime import datetime
from fabnet.core.key_storage import FileBasedKeyStorage
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

#logger.setLevel(logging.DEBUG)

VALID_STORAGE = './tests/cert/test_keystorage.zip'
PASSWD = 'qwerty123'

class EchoOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    def before_resend(self, packet):
        pass

    def process(self, packet):
        return FabnetPacketResponse(ret_code=0, ret_message='ok', binary_data=packet.binary_data)

    def callback(self, packet, sender):
        open('/tmp/big_message.out', 'w').write(packet.binary_data)


class TestAbstractOperator(unittest.TestCase):
    def test_big_data_test(self):
        keystorage = FileBasedKeyStorage(VALID_STORAGE, PASSWD)

        try:
            operator = Operator('127.0.0.1:1986', key_storage=keystorage)
            operator.neighbours = ['127.0.0.1:1987']
            operator.register_operation('ECHO', EchoOperation)
            server1 = FriServer('0.0.0.0', 1986, operator, 10, 'node_1', keystorage)
            ret = server1.start()
            self.assertEqual(ret, True)

            operator = Operator('127.0.0.1:1987', key_storage=keystorage)
            operator.neighbours = ['127.0.0.1:1986']
            operator.register_operation('ECHO', EchoOperation)
            server2 = FriServer('0.0.0.0', 1987, operator, 10, 'node_2', keystorage)
            ret = server2.start()
            self.assertEqual(ret, True)

            print 'Init big data...'
            data = '0123456789'*900000
            print 'Done'
            data += ''
            packet = { 'message_id': 323232,
                        'method': 'ECHO',
                        'sync': False,
                        'sender': '127.0.0.1:1987',
                        'binary_data': data}
            t0 = datetime.now()
            packet_obj = FabnetPacketRequest(**packet)
            operator.call_node('127.0.0.1:1986', packet_obj)


            operator.wait_response(323232, 20)
            print 'Echo big data time: %s'%(datetime.now()-t0)
            time.sleep(1)
            rcv_data = open('/tmp/big_message.out').read()
            self.assertEqual(len(rcv_data), len(data))
            os.remove('/tmp/big_message.out')
        finally:
            if server1:
                server1.stop()
            if server2:
                server2.stop()

if __name__ == '__main__':
    unittest.main()

