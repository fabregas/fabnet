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
from fabnet.utils.logger import oper_logger as logger
from fabnet.utils.internal import total_seconds

from fabnet.monitor.constants import DEFAULT_MONITOR_CONFIG, UP, DOWN
from fabnet.monitor.topology_cognition_mon import TopologyCognitionMon
from fabnet.monitor.notify_operation_mon import NotifyOperationMon
from fabnet.monitor.monitor_dbapi import PostgresDBAPI, AbstractDBAPI 

OPERLIST = [NotifyOperationMon, TopologyCognitionMon]

class MonitorOperator(Operator):
    OPTYPE = 'Monitor'

    def __init__(self, self_address, home_dir='/tmp/', key_storage=None, is_init_node=False, node_name='unknown', config={}):
        cur_cfg = {}
        cur_cfg.update(DEFAULT_MONITOR_CONFIG)
        cur_cfg.update(config)

        Operator.__init__(self, self_address, home_dir, key_storage, is_init_node, node_name, cur_cfg)

        self.__db_conn_str = None
        self.__db_api = None
        self.check_database()

        if key_storage:
            cert = key_storage.get_node_cert()
            ckey = key_storage.get_node_cert_key()
        else:
            cert = ckey = None
        client = FriClient(bool(cert), cert, ckey)

        self.__collect_up_nodes_stat_thread = CollectNodeStatisticsThread(self, client, UP)
        self.__collect_up_nodes_stat_thread.setName('%s-UP-CollectNodeStatisticsThread'%self.node_name)
        self.__collect_up_nodes_stat_thread.start()

        self.__collect_dn_nodes_stat_thread = CollectNodeStatisticsThread(self, client, DOWN)
        self.__collect_dn_nodes_stat_thread.setName('%s-DN-CollectNodeStatisticsThread'%self.node_name)
        self.__collect_dn_nodes_stat_thread.start()

        self.__discovery_topology_thrd = DiscoverTopologyThread(self)
        self.__discovery_topology_thrd.setName('%s-DiscoverTopologyThread'%self.node_name)
        self.__discovery_topology_thrd.start()

    def check_database(self):
        db_conn_str = Config.get('db_conn_str', self.OPTYPE)
        if self.__db_api and db_conn_str == self.__db_conn_str:
            return
        
        if self.__db_api:
            try:
                self.__db_api.close()
            except Exception, err:
                logger.error('DBAPI closing failed with error "%s"'%err)

        db_engine = Config.get('db_engine', self.OPTYPE)
        if db_engine is None:
            self.__db_api = AbstractDBAPI()
        elif db_engine == 'postgresql':
            self.__db_api = PostgresDBAPI(db_conn_str)
        elif db_engine == 'mongodb':
            self.__db_api = MongoDBAPI(db_conn_str)
        self.__db_conn_str = db_conn_str

    def stop_inherited(self):
        self.__collect_up_nodes_stat_thread.stop()
        self.__collect_dn_nodes_stat_thread.stop()
        self.__discovery_topology_thrd.stop()

        self.__discovery_topology_thrd.join()
        self.__db_api.close()

    def get_nodes_list(self, status=UP):
        return self.__db_api.get_nodes_list(status)

    def change_node_status(self, nodeaddr, status):
        self.__db_api.change_node_status(nodeaddr, status)

    def update_node_info(self, nodeaddr, node_name, home_dir, node_type, superior_neighbours, upper_neighbours):
        self.__db_api.update_node_info(nodeaddr, node_name, home_dir, node_type, \
                superior_neighbours, upper_neighbours)

    def update_node_stat(self, nodeaddr, stat):
        self.__db_api.update_node_stat(nodeaddr, stat)

    def notification(self, notify_provider, notify_type, notify_topic, message, date):
        self.__db_api.notification(notify_provider, notify_type, notify_topic, message, date)


class CollectNodeStatisticsThread(threading.Thread):
    def __init__(self, operator, client, check_status=UP):
        threading.Thread.__init__(self)
        self.operator = operator
        self.client = client
        self.check_status = check_status
        self.stopped = threading.Event()

    def run(self):
        logger.info('Thread started!')

        while not self.stopped.is_set():
            dt = 0
            try:
                t0 = datetime.now()
                logger.debug('Collecting %s nodes statistic...'%self.check_status)
                nodeaddrs = self.operator.get_nodes_list(self.check_status)

                for nodeaddr in nodeaddrs:
                    logger.debug('Get statistic from %s'%nodeaddr)

                    packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
                    ret_packet = self.client.call_sync(nodeaddr, packet_obj)
                    if self.check_status == UP and ret_packet.ret_code:
                        logger.warning('Node with address %s does not response... Details: %s'%(nodeaddr, ret_packet))
                        self.operator.change_node_status(nodeaddr, DOWN)
                    else:
                        stat = json.dumps(ret_packet.ret_parameters)
                        self.operator.update_node_stat(nodeaddr, stat)

                dt = total_seconds(datetime.now() - t0)
                logger.info('Nodes (with status=%s) stat is collected. Processed secs: %s'%(self.check_status, dt))
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

                self.operator.check_database()

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
