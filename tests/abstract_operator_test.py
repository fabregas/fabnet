import unittest
import time
import os
import logging
import json
from fabnet.core import constants
constants.CHECK_NEIGHBOURS_TIMEOUT = 1
from fabnet.core.fri_base import FriServer, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.operator_base import Operator, OperationBase
from fabnet.utils.logger import logger

logger.setLevel(logging.DEBUG)


class EchoOperation(OperationBase):
    def before_resend(self, packet):
        pass

    def process(self, packet):
        open('/tmp/server1.out', 'w').write(json.dumps(packet.to_dict()))
        return FabnetPacketResponse(ret_code=0, ret_message='ok', ret_parameters={'message': packet.parameters['message']})

    def callback(self, packet, sender):
        open('/tmp/server2.out', 'w').write(json.dumps(packet.to_dict()))


class TestAbstractOperator(unittest.TestCase):
    def test_echo_operation(self):
        server1 = server2 = None
        try:
            operator = Operator('127.0.0.1:1986')
            operator.neighbours = ['127.0.0.1:1987']
            operator.register_operation('ECHO', EchoOperation)
            server1 = FriServer('0.0.0.0', 1986, operator, server_name='node_1')
            ret = server1.start()
            self.assertEqual(ret, True)

            operator = Operator('127.0.0.1:1987')
            operator.neighbours = ['127.0.0.1:1986']
            operator.register_operation('ECHO', EchoOperation)
            server2 = FriServer('0.0.0.0', 1987, operator, server_name='node_2')
            ret = server2.start()
            self.assertEqual(ret, True)

            packet = { 'message_id': 323232,
                        'method': 'ECHO',
                        'sync': False,
                        'sender': '127.0.0.1:1987',
                        'parameters': {'message': 'test message'}}
            packet_obj = FabnetPacketRequest(**packet)
            rcode, rmsg = operator.call_node('127.0.0.1:1986', packet_obj)
            self.assertEqual(rcode, 0, rmsg)

            time.sleep(1)

            self.assertEqual(os.path.exists('/tmp/server1.out'), True)
            self.assertEqual(os.path.exists('/tmp/server2.out'), True)

            request = json.loads(open('/tmp/server1.out').read())
            response = json.loads(open('/tmp/server2.out').read())
            self.assertEqual(request, packet)
            good_resp = {'message_id': 323232,
                'ret_code': 0,
                'ret_message': 'ok',
                'ret_parameters': {'message': 'test message'}}
            self.assertEqual(response, good_resp)
        finally:
            if server1:
                server1.stop()
            if server2:
                server2.stop()


if __name__ == '__main__':
    unittest.main()

