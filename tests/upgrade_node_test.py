import unittest
import time
import os
import logging
import signal
import json
import random
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.operator import Operator
from fabnet.utils.logger import logger
import subprocess

logger.setLevel(logging.DEBUG)

ORIG_REPO = os.path.abspath('./.git')

class TestUpgradeNode(unittest.TestCase):
    def test_node_upgrade(self):
        server_proc = None
        try:
            os.system('rm -rf /tmp/fabnet_node_code')
            os.system('rm -rf /tmp/upgrade_node.log')
            os.system('git clone %s /tmp/fabnet_node_code/'%ORIG_REPO)
            os.chdir('/tmp/fabnet_node_code/')
            #os.system('git reset --hard f797284a29fcbaa08d8ff68d07e5e6e352a95fe3')
            os.system('find . -name *.pyc -delete')

            address = '127.0.0.1:1987'
            args = ['/usr/bin/python', '/tmp/fabnet_node_code/fabnet/bin/fabnet-node', address, 'init-fabnet', 'test_upgr_node', '/tmp', 'DHT']
            args.append('--nodaemon')
            server_proc = subprocess.Popen(args)
            time.sleep(2)

            os.system("echo '{\"DHT\": 100500}' > UPGRADE_VERSION")
            packet_obj = FabnetPacketRequest(method='UpgradeNode', parameters={'origin_repo_url': 'bad_url'}, sync=True)
            client = FriClient()
            ret = client.call_sync('127.0.0.1:1987', packet_obj)
            self.assertEqual(ret.ret_code, 0)

            print 'upgrading /tmp/fabnet_node_code from %s'%ORIG_REPO
            packet_obj = FabnetPacketRequest(method='UpgradeNode', parameters={'origin_repo_url': ORIG_REPO}, sync=True)
            client = FriClient()
            ret = client.call_sync('127.0.0.1:1987', packet_obj)
            self.assertEqual(ret.ret_code, 0)
            self.assertTrue(os.path.exists('/tmp/upgrade_node.log'))
            self.assertTrue('successfully' in open('/tmp/upgrade_node.log').read())
        finally:
            os.system('rm -rf /tmp/fabnet_node_code')
            if server_proc:
                server_proc.send_signal(signal.SIGINT)
                server_proc.wait()


if __name__ == '__main__':
    unittest.main()

