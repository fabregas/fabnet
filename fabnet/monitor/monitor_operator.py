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

from fabnet.core.fri_base import FriClient, FabnetPacketRequest
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.utils.db_conn import DBOperationalException, DBEmptyResult
from fabnet.core.operator import Operator
from fabnet.utils.logger import logger
from fabnet.utils.internal import total_seconds

from fabnet.monitor.topology_cognition_mon import TopologyCognitionMon

MONITOR_DB = 'fabnet_monitor_db'

UP = 1
DOWN = 0

COLLECT_NODES_STAT_TIMEOUT = 30

#discovery topology thread constants
NO_TOPOLOGY_DYSCOVERY_WINDOW = 10
MIN_TOPOLOGY_DISCOVERY_WAIT = 1
MAX_TOPOLOGY_DISCOVERY_WAIT = 10
DISCOVERY_TOPOLOGY_TIMEOUT = 30

OPERMAP =  {'TopologyCognition': TopologyCognitionMon}

class MonitorOperator(Operator):
    OPTYPE = 'Monitor'

    def __init__(self, self_address, home_dir='/tmp/', certfile=None, is_init_node=False, node_name='unknown'):
        Operator.__init__(self, self_address, home_dir, certfile, is_init_node, node_name)

        self.__monitor_db_path = "dbname=%s user=postgres"%MONITOR_DB
        self._conn = self._init_db()

        self.__collect_nodes_stat_thread = CollectNodeStatisticsThread(self)
        self.__collect_nodes_stat_thread.setName('%s-CollectNodeStatisticsThread'%self.node_name)
        self.__collect_nodes_stat_thread.start()

        self.__discovery_topology_thrd = DiscoverTopologyThread(self)
        self.__discovery_topology_thrd.setName('%s-DiscoverTopologyThread'%self.node_name)
        self.__discovery_topology_thrd.start()

    def stop(self):
        self.__collect_nodes_stat_thread.stop()
        self.__discovery_topology_thrd.stop()

        Operator.stop(self)

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


    def update_node_info(self, nodeaddr, node_name, superior_neighbours, upper_neighbours):
        superiors = ','.join(superior_neighbours)
        uppers = ','.join(upper_neighbours)
        rows = self._conn.select("SELECT id FROM nodes_info WHERE node_address=%s", (nodeaddr, ))
        if not rows:
            self._conn.execute("INSERT INTO nodes_info (node_address, node_name, status, superiors, uppers) \
                                VALUES (%s, %s, %s, %s, %s)", (nodeaddr, node_name, UP, superiors, uppers))
        else:
            self._conn.execute("UPDATE nodes_info SET status=%s, superiors=%s, uppers=%s WHERE id=%s", \
                                (UP, superiors, uppers, rows[0][0]))


    def update_node_stat(self, nodeaddr, stat):
        self._conn.execute("UPDATE nodes_info SET status=%s, statistic=%s, last_check=%s \
                            WHERE node_address=%s", (UP, stat, datetime.now(), nodeaddr))


    def on_network_notify(self, notify_type, notify_provider, notify_topic, message):
        """This method should be imlemented for some actions
            on received network nofitications
        """
        logger.info('[NOTIFICATION][%s][%s] *%s* %s'%(notify_type, notify_provider, notify_topic, message))
        self._conn.execute("INSERT INTO notification (node_address, notify_type, notify_topic, notify_msg, notify_dt) VALUES (%s, %s, %s, %s, %s)", \
                    (notify_provider, notify_type, notify_topic, message, datetime.now()))

        if notify_topic == 'NodeUp':
            self.change_node_status(notify_provider, UP)
        elif notify_topic == 'NodeDown':
            self.change_node_status(notify_provider, DOWN)


class CollectNodeStatisticsThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = True

    def run(self):
        self.stopped = False
        logger.info('Thread started!')

        client = FriClient()
        while not self.stopped:
            dt = 0
            try:
                t0 = datetime.now()
                logger.info('Collecting nodes statistic...')
                nodeaddrs = self.operator.get_nodes_list()

                for nodeaddr in nodeaddrs:
                    logger.debug('Get statistic from %s'%nodeaddr)

                    packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
                    ret_packet = client.call_sync(nodeaddr, packet_obj)
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
                wait_time = COLLECT_NODES_STAT_TIMEOUT - dt
                if wait_time > 0:
                    for i in xrange(int(wait_time)):
                        if self.stopped:
                            break
                        time.sleep(1)
                    time.sleep(wait_time - int(wait_time))

        logger.info('Thread stopped!')

    def stop(self):
        self.stopped = True


class DiscoverTopologyThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = True

    def run(self):
        self.stopped = False
        logger.info('Thread started!')

        while not self.stopped:
            try:
                try:
                    tc_oper = self.operator.get_operation_instance('TopologyCognition')
                except OperException, err:
                    time.sleep(1)
                    continue

                while True:
                    last_processed_dt = tc_oper.get_last_processed_dt()
                    dt = datetime.now() - last_processed_dt
                    if total_seconds(dt) < NO_TOPOLOGY_DYSCOVERY_WINDOW:
                        w_seconds = random.randint(MIN_TOPOLOGY_DISCOVERY_WAIT, MAX_TOPOLOGY_DISCOVERY_WAIT)
                        for i in xrange(w_seconds):
                            time.sleep(1)
                            if self.stopped:
                                return
                    else:
                        break

                logger.info('Starting topology discovery...')
                packet = FabnetPacketRequest(method='TopologyCognition')# parameters={"need_rebalance": 1})
                self.operator.call_network(packet)

                for i in xrange(DISCOVERY_TOPOLOGY_TIMEOUT):
                    time.sleep(1)
                    if self.stopped:
                        return
            except Exception, err:
                logger.error(str(err))

        logger.info('Thread stopped!')


    def stop(self):
        self.stopped = True


MonitorOperator.update_operations_map(OPERMAP)
