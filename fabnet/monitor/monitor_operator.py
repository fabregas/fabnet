#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.monitor.monitor_operator

@author Konstantin Andrusenko
@date November 11, 2012
"""
import os
import time
import threading
import random
import json
from datetime import datetime
from Queue import Queue

from fabnet.core.fri_base import  FabnetPacketRequest
from fabnet.core.fri_client import FriClient
from fabnet.core.operator import Operator
from fabnet.core.config import Config
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.utils.db_conn import DBOperationalException, DBEmptyResult
from fabnet.utils.logger import oper_logger as logger
from fabnet.utils.internal import total_seconds

from fabnet.monitor.constants import DEFAULT_MONITOR_CONFIG, MONITOR_DB, UP, DOWN
from fabnet.monitor.topology_cognition_mon import TopologyCognitionMon
from fabnet.monitor.notify_operation_mon import NotifyOperationMon

OPERLIST = [NotifyOperationMon, TopologyCognitionMon]

class MonitorOperator(Operator):
    OPTYPE = 'Monitor'

    def __init__(self, self_address, home_dir='/tmp/', key_storage=None, is_init_node=False, node_name='unknown', config={}):
        Operator.__init__(self, self_address, home_dir, key_storage, is_init_node, node_name, config)

        Config.update_config(DEFAULT_MONITOR_CONFIG)
        Config.update_config(config)

        self.__monitor_db_path = "dbname=%s user=postgres"%MONITOR_DB
        self._conn = self._init_db()

        if key_storage:
            cert = key_storage.get_node_cert()
            ckey = key_storage.get_node_cert_key()
        else:
            cert = ckey = None
        client = FriClient(bool(cert), cert, ckey)

        self.__collect_nodes_stat_thread = CollectNodeStatisticsThread(self, client)
        self.__collect_nodes_stat_thread.setName('%s-CollectNodeStatisticsThread'%self.node_name)
        self.__collect_nodes_stat_thread.start()

        self.__discovery_topology_thrd = DiscoverTopologyThread(self)
        self.__discovery_topology_thrd.setName('%s-DiscoverTopologyThread'%self.node_name)
        self.__discovery_topology_thrd.start()

    def stop_inherited(self):
        self.__collect_nodes_stat_thread.stop()
        self.__discovery_topology_thrd.stop()

        self.__collect_nodes_stat_thread.join()
        self.__discovery_topology_thrd.join()
        self._conn.close()

    def _init_db(self):
        os.system('createdb -U postgres %s'%MONITOR_DB)
        conn = DBConnection(self.__monitor_db_path)

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
            conn.close()
            raise err

        return conn


    def get_nodes_list(self):
        try:
            return self._conn.select_col("SELECT node_address FROM nodes_info WHERE status=%s", (UP,))
        except DBEmptyResult:
            return []


    def change_node_status(self, nodeaddr, status):
        rows = self._conn.select("SELECT id FROM nodes_info WHERE node_address=%s", (nodeaddr, ))
        if not rows:
            self._conn.execute("INSERT INTO nodes_info (node_address, node_name, status) VALUES (%s, %s, %s)", (nodeaddr, '', status))
        else:
            self._conn.execute("UPDATE nodes_info SET status=%s WHERE id=%s", (status, rows[0][0]))


    def update_node_info(self, nodeaddr, node_name, home_dir, node_type, superior_neighbours, upper_neighbours):
        superiors = ','.join(superior_neighbours)
        uppers = ','.join(upper_neighbours)
        rows = self._conn.select("SELECT id, node_name, home_dir, node_type, status, superiors, uppers \
                                    FROM nodes_info WHERE node_address=%s", (nodeaddr, ))
        if not rows:
            self._conn.execute("INSERT INTO nodes_info (node_address, node_name, home_dir, node_type, status, superiors, uppers) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s)", (nodeaddr, node_name, home_dir, node_type, UP, superiors, uppers))
        else:
            if rows[0][1:] == (node_name, home_dir, node_type, UP, superiors, uppers):
                return
            self._conn.execute("UPDATE nodes_info \
                                SET node_name=%s, home_dir=%s, node_type=%s, status=%s, superiors=%s, uppers=%s \
                                WHERE id=%s", \
                                (node_name, home_dir, node_type, UP, superiors, uppers, rows[0][0]))


    def update_node_stat(self, nodeaddr, stat):
        self._conn.execute("UPDATE nodes_info SET status=%s, statistic=%s, last_check=%s \
                            WHERE node_address=%s", (UP, stat, datetime.now(), nodeaddr))



class CollectNodeStatisticsThread(threading.Thread):
    def __init__(self, operator, client):
        threading.Thread.__init__(self)
        self.operator = operator
        self.client = client
        self.stopped = threading.Event()

    def run(self):
        logger.info('Thread started!')

        while not self.stopped.is_set():
            dt = 0
            try:
                t0 = datetime.now()
                logger.info('Collecting nodes statistic...')
                nodeaddrs = self.operator.get_nodes_list()

                for nodeaddr in nodeaddrs:
                    logger.debug('Get statistic from %s'%nodeaddr)

                    packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
                    ret_packet = self.client.call_sync(nodeaddr, packet_obj)
                    if ret_packet.ret_code:
                        self.operator.change_node_status(nodeaddr, DOWN)
                    else:
                        stat = json.dumps(ret_packet.ret_parameters)
                        self.operator.update_node_stat(nodeaddr, stat)


                dt = total_seconds(datetime.now() - t0)
                logger.info('Nodes stat is collected. Processed secs: %s'%dt)
            except Exception, err:
                logger.error(str(err))
            finally:
                wait_time = Config.COLLECT_NODES_STAT_TIMEOUT - dt
                if wait_time > 0:
                    for i in xrange(int(wait_time)):
                        if self.stopped.is_set():
                            break
                        time.sleep(1)
                    time.sleep(wait_time - int(wait_time))

        logger.info('Thread stopped!')

    def stop(self):
        self.stopped.set()


class DiscoverTopologyThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = threading.Event()
        self._self_discovery = True
        self._nodes_queue = Queue()

    def run(self):
        logger.info('Thread started!')

        while True:
            try:
                for i in xrange(Config.DISCOVERY_TOPOLOGY_TIMEOUT):
                    if self.stopped.is_set():
                        break
                    time.sleep(1)

                if self.stopped.is_set():
                    break

                from_addr = self.next_discovery_node()
                if from_addr:
                    logger.info('Starting topology discovery from %s...'%from_addr)
                    params = {"need_rebalance": 1}
                else:
                    logger.info('Starting topology discovery from this node...')
                    params = {}

                packet = FabnetPacketRequest(method='TopologyCognition', parameters=params)
                self.operator.call_network(packet, from_addr)
            except Exception, err:
                logger.error(str(err))

        logger.info('Thread stopped!')


    def next_discovery_node(self):
        if self._self_discovery:
           from_address = None
        else:
            if self._nodes_queue.empty():
                for nodeaddr in self.operator.get_nodes_list():
                    self._nodes_queue.put(nodeaddr)
            if self._nodes_queue.empty():
                from_address = None
            else:
                from_address = self._nodes_queue.get()

        self._self_discovery = not self._self_discovery
        return from_address


    def stop(self):
        self.stopped.set()


MonitorOperator.update_operations_list(OPERLIST)
