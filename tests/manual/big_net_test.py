#!/usr/bin/python
import unittest
import time
import os
import logging
import threading
import json
import random
import subprocess
import signal
import shutil
from fabnet.utils.logger import logger

from fabnet.utils.safe_json_file import SafeJsonFile
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.core.fri_client import FriClient
logger.setLevel(logging.DEBUG)

NODES_COUNT = range(90, 91)

class TestBigNework(unittest.TestCase):
    def test_00_create_network(self):
        for count in NODES_COUNT:
            print 'Starting %s nodes...'%count
            self.create_net(count)
            #os.system('python ./tests/topology_to_tgf /tmp/fabnet_topology.db /tmp/fabnet_topology.%s.tgf'%count)

    def create_net(self, nodes_count):
        PROCESSES = []
        addresses = []
        try:
            for i in range(1900, 1900+nodes_count):
                address = '127.0.0.1:%s'%i
                if not addresses:
                    n_node = 'fake:9999'
                else:
                    n_node = random.choice(addresses)

                addresses.append(address)

                home = '/tmp/node_%s'%i
                if os.path.exists(home):
                    shutil.rmtree(home)
                os.mkdir(home)
                logger.warning('{SNP} STARTING NODE %s'%address)
                p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, '%.02i'%i, home, 'DHT'])
                logger.warning('{SNP} PROCESS STARTED')
                time.sleep(.5)

                PROCESSES.append(p)
                if len(addresses) > 3:
                    self._check_stat(address)


            p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fri-caller', 'TopologyCognition', address])
            node_i = address.split(':')[-1]
            self._wait_topology(node_i, len(addresses))
            os.system('python ./tests/topology_to_tgf /tmp/node_%s/fabnet_topology.db /tmp/fabnet_topology.%s-orig.tgf'%(node_i, len(addresses)))


            for address in addresses:
                print 'Collecting topology from %s ...'%address
                p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fri-caller', 'TopologyCognition', address, '{"need_rebalance": 1}'])

                node_i = address.split(':')[-1]
                self._wait_topology(node_i, nodes_count)

                os.system('python ./tests/topology_to_tgf /tmp/node_%s/fabnet_topology.db /tmp/fabnet_topology.%s-balanced.tgf'%(node_i, nodes_count,))

            print 'TOPOLOGY REBALANCED'

            return

            del_count = nodes_count/3
            for i in xrange(del_count):
                process = random.choice(PROCESSES)
                idx = PROCESSES.index(process)
                del_address = addresses[idx]
                print 'STOPPING %s'%del_address
                del PROCESSES[idx]
                del addresses[idx]
                process.send_signal(signal.SIGINT)
                process.wait()

            time.sleep(80)

            address = random.choice(addresses)
            print 'Collecting topology from %s ...'%address
            p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fri-caller', 'TopologyCognition', address])
            time.sleep(1)
            node_i = address.split(':')[-1]
            self._wait_topology(node_i, nodes_count-del_count)
            os.system('python ./tests/topology_to_tgf /tmp/node_%s/fabnet_topology.db /tmp/fabnet_topology.%s-%s.tgf'% \
                        (node_i, nodes_count, del_count))

        finally:
            for process in PROCESSES:
                process.send_signal(signal.SIGINT)

            print 'SENDED SIGNALS'
            for process in PROCESSES:
                process.wait()

            print 'STOPPED'

    def _check_stat(self, address):
        client = FriClient()

        while True:
            try:
                packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
                ret_packet = client.call_sync(address, packet_obj)
                if ret_packet.ret_code:
                    time.sleep(.5)
                    continue

                uppers_balance = int(ret_packet.ret_parameters[u'uppers_balance'])
                superiors_balance = int(ret_packet.ret_parameters[u'superiors_balance'])
                if uppers_balance >= 0 and superiors_balance >= 0:
                    break
                print 'Node %s is not balanced yet! Waiting...'%address
                time.sleep(.5)
            except Exception, err:
                logger.error('ERROR: %s'%err)
                raise err



    def _wait_topology(self, node_i, nodes_count):
        conn = None
        while True:
            try:
                db = '/tmp/node_%s/fabnet_topology.db'%node_i
                while not os.path.exists(db):
                    print '%s not exists!'%db
                    time.sleep(0.1)

                time.sleep(.5)
                data = SafeJsonFile(db).read()
                cnt = 0
                for value in data.values():
                    if int(value.get('old_data', 0)) == 0:
                        cnt += 1

                if cnt != nodes_count:
                    time.sleep(.5)
                else:
                    break
            finally:
                if conn:
                    curs.close()
                    conn.close()


if __name__ == '__main__':
    unittest.main()

