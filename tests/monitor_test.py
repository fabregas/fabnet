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
import subprocess
import signal
from fabnet.core.config import Config
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER, ET_INFO, ET_ALERT
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.utils.db_conn import DBOperationalException, DBEmptyResult
from fabnet.monitor.monitor_operator import MonitorOperator
from fabnet.dht_mgmt.dht_operator import DHTOperator

from fabnet.utils.logger import logger

logger.setLevel(logging.DEBUG)

PROCESSES = []
ADDRESSES = []
DEBUG = False

MONITOR_DB = 'fabnet_monitor_db'

class TestMonitorNode(unittest.TestCase):
    def create_net(self, nodes_count):
        global PROCESSES
        global ADDRESSES

        for unuse in range(nodes_count):
            if not ADDRESSES:
                n_node = 'init-fabnet'
                i = 1900
            else:
                n_node = random.choice(ADDRESSES)
                i = int(ADDRESSES[-1].split(':')[-1])+1
                self._wait_node(n_node)

            address = '127.0.0.1:%s'%i
            ADDRESSES.append(address)

            home = '/tmp/node_%s'%i
            if os.path.exists(home):
                shutil.rmtree(home)
            os.mkdir(home)

            logger.warning('{SNP} STARTING NODE %s'%address)
            if n_node == 'init-fabnet':
                ntype = 'Monitor'
                Config.load(os.path.join(home, 'node_config'))
                Config.update_config({'db_engine': 'postgresql', \
                    'db_conn_str': "dbname=%s user=postgres"%MONITOR_DB}, 'Monitor')
            else:
                ntype = 'DHT'
            args = ['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, '%.02i'%i, home, ntype, '--nodaemon']
            if DEBUG:
                args.append('--debug')
            p = subprocess.Popen(args)
            logger.warning('{SNP} PROCESS STARTED')
            time.sleep(1)

            PROCESSES.append(p)
            #if len(ADDRESSES) > 2:
            #    self._check_stat(address)

        for address in ADDRESSES:
            self._check_stat(address)

        time.sleep(1.5)
        print 'NETWORK STARTED'

    def _wait_node(self, node):
        client = FriClient()
        while True:
            packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
            ret_packet = client.call_sync(node, packet_obj)
            if ret_packet.ret_code:
                print 'Node does not init FRI server yet. Waiting it...'
                time.sleep(.5)
                continue
            break

    def _check_stat(self, address):
        client = FriClient()

        while True:
            try:
                packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
                ret_packet = client.call_sync(address, packet_obj)
                if ret_packet.ret_code:
                    time.sleep(.5)
                    continue

                uppers_balance = int(ret_packet.ret_parameters['NeighboursInfo'][u'uppers_balance'])
                superiors_balance = int(ret_packet.ret_parameters['NeighboursInfo'][u'superiors_balance'])
                if uppers_balance >= 0 and superiors_balance >= 0:
                    return
                else:
                    print 'Node %s is not balanced yet! Waiting...'%address
                time.sleep(.5)
            except Exception, err:
                logger.error('ERROR: %s'%err)
                raise err

    def test00_initnet(self):
        os.system('dropdb -U postgres %s'%MONITOR_DB)
        os.system('createdb -U postgres %s'%MONITOR_DB)

        conn = DBConnection("dbname=%s user=postgres"%MONITOR_DB)
        conn.select('select 1')
        try:
            notification_tbl = """CREATE TABLE notification (
                id serial PRIMARY KEY,
                node_address varchar(512) NOT NULL,
                notify_type varchar(64) NOT NULL,
                notify_topic varchar(512),
                notify_msg text,
                notify_dt timestamp
            )"""

            nodes_info_tbl = """CREATE TABLE nodes_info (
                id serial PRIMARY KEY,
                node_address varchar(512) UNIQUE NOT NULL,
                node_name varchar(128) NOT NULL,
                node_type varchar(128),
                home_dir varchar(1024),
                status integer NOT NULL DEFAULT 0,
                superiors text,
                uppers text,
                statistic text,
                last_check timestamp
            )"""

            try:
                conn.select("SELECT id FROM nodes_info WHERE id=1")
            except DBOperationalException:
                conn.execute(nodes_info_tbl)

           
            try:
                conn.select("SELECT id FROM notification WHERE id=1")
            except DBOperationalException:
                conn.execute(notification_tbl)
        except Exception, err:
            raise err
        finally:
            conn.close()

        self.create_net(4)

    def test99_stopnet(self):
        for process in PROCESSES:
            process.send_signal(signal.SIGINT)
        print 'SENDED SIGNALS'
        for process in PROCESSES:
            process.wait()
        print 'STOPPED'

    def test01_monitor(self):
        qeury_notify = 'SELECT notify_type, node_address, notify_msg FROM notification WHERE notify_topic=%s'
        qeury_nodes = 'SELECT node_address, node_name, status, superiors, uppers, statistic, last_check FROM nodes_info'
        conn = DBConnection("dbname=%s user=postgres"%MONITOR_DB)
        events = conn.select(qeury_notify, ('NodeUp',))
        self.assertEqual(len(events), 3, events)

        nodes_info = conn.select(qeury_nodes)
        self.assertEqual(len(nodes_info), 3, nodes_info)
        self.assertTrue(len(nodes_info[0][0])>0, nodes_info[0][0])
        self.assertTrue(len(nodes_info[0][1])==0, nodes_info[0][1])
        self.assertTrue(nodes_info[0][2]==1, nodes_info[0][2])
        self.assertTrue(nodes_info[0][3]==None, nodes_info[0][3])
        self.assertTrue(nodes_info[0][4]==None, nodes_info[0][4])
        self.assertTrue(nodes_info[0][5]==None, nodes_info[0][5])
        self.assertTrue(nodes_info[0][6]==None, nodes_info[0][6])

        p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fri-caller', 'TopologyCognition', ADDRESSES[0], '{}', 'async'])
        time.sleep(2)

        nodes_info = conn.select(qeury_nodes)
        self.assertEqual(len(nodes_info), 4, nodes_info)

        nodes_info = conn.select(qeury_nodes+' WHERE node_address=%s', (ADDRESSES[0],))
        self.assertEqual(len(nodes_info), 1, nodes_info)
        self.assertEqual(nodes_info[0][0], ADDRESSES[0])
        self.assertEqual(nodes_info[0][1], '1900')
        self.assertTrue(nodes_info[0][2]==1, nodes_info[0][2])
        self.assertTrue(len(nodes_info[0][3])>0, nodes_info[0][3])
        self.assertTrue(len(nodes_info[0][4])>0, nodes_info[0][4])
        self.assertTrue(nodes_info[0][5]==None, nodes_info[0][5])
        self.assertTrue(nodes_info[0][6]==None, nodes_info[0][6])




if __name__ == '__main__':
    unittest.main()

