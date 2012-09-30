import unittest
import time
import os
import logging
import json
from fabnet.core import constants
constants.CHECK_NEIGHBOURS_TIMEOUT = 1
from fabnet.core.fri_base import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.operator import Operator
from fabnet.core.operation_base import OperationBase
from fabnet.utils.logger import logger
from datetime import datetime

#logger.setLevel(logging.DEBUG)


class EchoOperation(OperationBase):
    def before_resend(self, packet):
        pass

    def process(self, packet):
        return FabnetPacketResponse(ret_code=0, ret_message='ok', ret_parameters={'message': packet.parameters['message']})

    def callback(self, packet, sender):
        open('/tmp/big_message.out', 'w').write(packet.ret_parameters['message'])


class TestAbstractOperator(unittest.TestCase):
    def test_big_data_test(self):
        cert = './tests/cert/server.crt'
        key = './tests/cert/server.key'
        try:
            operator = Operator('127.0.0.1:1986', certfile=cert)
            operator.neighbours = ['127.0.0.1:1987']
            operator.register_operation('ECHO', EchoOperation)
            server1 = FriServer('0.0.0.0', 1986, operator, 10, 'node_1', cert, key)
            ret = server1.start()
            self.assertEqual(ret, True)

            operator = Operator('127.0.0.1:1987', certfile=cert)
            operator.neighbours = ['127.0.0.1:1986']
            operator.register_operation('ECHO', EchoOperation)
            server2 = FriServer('0.0.0.0', 1987, operator, 10, 'node_2', cert, key)
            ret = server2.start()
            self.assertEqual(ret, True)

            data = '0123456789'*1000000
            packet = { 'message_id': 323232,
                        'method': 'ECHO',
                        'sync': False,
                        'sender': '127.0.0.1:1987',
                        'parameters': {'message': data}}
            packet_obj = FabnetPacketRequest(**packet)
            rcode, rmsg = operator.call_node('127.0.0.1:1986', packet_obj)
            self.assertEqual(rcode, 0, rmsg)

            operator.wait_response(323232, 20)
            time.sleep(1)
            rcv_data = open('/tmp/big_message.out').read()
            self.assertEqual(len(rcv_data), len(data))
        finally:
            if server1:
                server1.stop()
            if server2:
                server2.stop()

if __name__ == '__main__':
    unittest.main()

