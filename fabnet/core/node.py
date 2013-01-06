#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.node
@author Konstantin Andrusenko
@date September 7, 2012

This module contains the Node class implementation
"""
import os
import time
import threading

from fabnet.core.fri_server import FriServer
from fabnet.core.fri_client import FriClient
from fabnet.settings import OPERATORS_MAP, DEFAULT_OPERATOR
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.core.key_storage import init_keystore
from fabnet.core.operator import OperatorProcess, OperatorClient
from fabnet.core.operations_processor import OperationsProcessor
from fabnet.core.operations_manager import OperationsManager
from fabnet.core.workers_manager import WorkersManager
from fabnet.core.constants import ET_INFO, STAT_OSPROC_TIMEOUT
from fabnet.core.statistic import OSProcessesStatisticCollector
from fabnet.utils.logger import logger


class Node:
    def __init__(self, hostname, port, home_dir, node_name='anonymous_node',
                    ks_path=None, ks_passwd=None, node_type=None, bind_host='0.0.0.0'):
        self.hostname = hostname
        self.bind_host = bind_host
        self.port = port
        self.home_dir = home_dir
        self.node_name = node_name
        self.node_type = node_type
        self.oper_client = OperatorClient(self.node_name)
        if ks_path:
            self.keystore = init_keystore(ks_path, ks_passwd)
        else:
            self.keystore = None

        self.server = None
        self.operator_process = None
        self.osproc_stat = None
        cur_thread = threading.current_thread()
        cur_thread.setName('%s-main'%self.node_name)

    def start(self, neighbour):
        address = '%s:%s' % (self.hostname, self.port)
        if not neighbour:
            is_init_node = True
        else:
            is_init_node = False

        operator_class = OPERATORS_MAP.get(self.node_type, None)
        if operator_class is None:
            logger.error('Node type "%s" does not found!'%self.node_type)
            return False

        op_proc = OperatorProcess(operator_class, address, self.home_dir, self.keystore, is_init_node, self.node_name)
        op_proc.start_carefully()

        try:
            oper_manager = OperationsManager(operator_class.OPERATIONS_LIST, self.node_name, self.keystore)
            workers_mgr = WorkersManager(OperationsProcessor, server_name=self.node_name, \
                                            init_params=(oper_manager, self.keystore))
            fri_server = FriServer(self.bind_host, self.port, workers_mgr, self.node_name)
            started = fri_server.start()
            if not started:
                raise Exception('FriServer does not started!')
        except Exception, err:
            time.sleep(1)
            op_proc.stop()
            op_proc.join()
            raise err

        self.server = fri_server
        self.operator_process = op_proc

        proc_pids = []
        proc_pids.append(('FriServer', os.getpid()))
        proc_pids.append(('Operator', op_proc.pid))
        for child in workers_mgr.iter_children():
            proc_pids.append(('OperationsProcessors', child.pid))
        self.osproc_stat = OSProcessesStatisticCollector(oper_manager.operator_cl, \
                                        proc_pids, [workers_mgr], STAT_OSPROC_TIMEOUT)
        self.osproc_stat.start()

        if is_init_node:
            return

        self.oper_client.discovery_neighbours(neighbour)


    def stop(self):
        try:
            address = '%s:%s'%(self.hostname, self.port)
            params = {'event_type': ET_INFO, 'event_topic': 'NodeDown', \
                        'event_message': 'Goodbye, fabnet :(', 'event_provider': address}
            packet = FabnetPacketRequest(method='NotifyOperation', parameters=params, sender=None, multicast=True)
            if self.keystore:
                cert = self.keystore.get_node_cert()
                ckey = self.keystore.get_node_cert_key()
            else:
                cert = ckey = None
            fri_client = FriClient(bool(cert), cert, ckey)
            fri_client.call(address, packet)
        except Exception, err:
            logger.warning('Cant send notification to network. Details: %s'%err)

        if self.osproc_stat:
            self.osproc_stat.stop()
        if self.server:
            self.server.stop()
            self.operator_process.stop()


