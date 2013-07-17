#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.mgmt.management_agent
@author Konstantin Andrusenko
@date July 17, 2013

This module contains the implementation of ManagementAgent class
"""
import os
import sys
import ssl
import threading
import signal
import time
import traceback
from subprocess import Popen, PIPE

from fabnet.core.key_storage import init_keystore
from fabnet.core.workers import ThreadBasedFriWorker 
from fabnet.core.fri_server import FriServer
from fabnet.core.socket_processor import SocketProcessor
from fabnet.core.workers_manager import WorkersManager
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_ERROR 
from fabnet.utils.logger import oper_logger as logger

NODE_DAEMON_BIN = os.path.abspath(os.path.join(os.path.dirname(__file__), '../bin/node-daemon'))
DEFAULT_MGMT_PORT = 1104


class MgmtCommandsProcessor(ThreadBasedFriWorker):
    def process(self, socket_processor):
        try:
            packet = socket_processor.recv_packet()

            if not (packet.is_request and packet.sync):
                raise Exception('Async operations for management agent does not supported!')

            ret_code, ret_msg = self.process_operation(packet)
            ret_packet = FabnetPacketResponse(ret_code=ret_code, ret_message=ret_msg)
            socket_processor.send_packet(ret_packet)
            socket_processor.close_socket(force=True)
        except Exception, err:
            ret_message = 'MgmtCommandsProcessor.process() error: %s' % err
            logger.write = logger.info
            traceback.print_exc(file=logger)
            try:
                if not socket_processor.is_closed():
                    err_packet = FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err))
                    socket_processor.send_packet(err_packet)
            except Exception, err:
                logger.error("Can't send error message to socket: %s"%err)
        finally:
            if socket_processor:
                socket_processor.close_socket(force=True)


    def process_operation(self, packet):
        method = packet.method
        node_home = packet.parameters.get('node_home', None)
        ks_pwd = packet.parameters.get('ks_pwd', None)
        if node_home is None:
            return RC_ERROR, 'node_home parameter is expected!'
        if ks_pwd is None:
            return RC_ERROR, 'ks_pwd parameter is expected!'

        if method == 'StartNode':
            node_name = packet.parameters.get('node_name', None)
            node_addr = packet.parameters.get('node_addr', None)
            neighbour_addr = packet.parameters.get('neighbour_addr', None)
            node_type = packet.parameters.get('node_type', None)
            return self.start_node(node_name, node_home, node_addr, neighbour_addr, node_type, ks_pwd)

        elif method == 'StopNode':
            return self.stop_node(node_home, ks_pwd)

        elif method == 'ReloadNode':
            return self.reload_node(node_home, ks_pwd)

        else:
            raise Exception('Unknown method "%s"'%method)

    def start_node(self, node_name, node_home, node_addr, neighbour_addr, node_type, ks_pwd):
        cmd = [NODE_DAEMON_BIN, 'start']
        if neighbour_addr:
            cmd += [neighbour_addr, node_addr, node_name, node_type]
            if node_name is None or node_type is None or node_addr is None:
                raise Exception('All node_name, node_addr and node_type parameters are expected!')
        cmd.append('--input-pwd')

        rcode, rmsg = self.__call(cmd, node_home, ks_pwd)
        return rcode, rmsg

    def stop_node(self, node_home, ks_pwd):
        self.__validate_pwd(node_home, ks_pwd)
        cmd = [NODE_DAEMON_BIN, 'stop']
        rcode, rmsg = self.__call(cmd, node_home)
        return rcode, rmsg

    def reload_node(self, node_home, ks_pwd):
        self.__validate_pwd(node_home, ks_pwd)
        cmd = [NODE_DAEMON_BIN, 'reload', '--input-pwd']
        rcode, rmsg = self.__call(cmd, node_home, ks_pwd)
        return rcode, rmsg

    def __call(self, cmd_list, node_home, input_s=None):
        proc = Popen(cmd_list, stdout=PIPE, stderr=PIPE, stdin=PIPE, env={'FABNET_NODE_HOME': node_home})
        out, err = proc.communicate(input_s)
        return proc.returncode, '%s\n%s'%(out, err)

    def __validate_pwd(self, home_dir, ks_pwd):
        keystore = ''
        for fname in os.listdir(home_dir):
            if fname.endswith('.ks'):
                keystore = os.path.join(home_dir, fname)
                break

        if not keystore:
            if not ks_pwd:
                return
            raise Exception('Key storage does not found at %s'%home_dir)
        init_keystore(keystore, ks_pwd)


class ManagementAgent:
    def __init__(self, hostname, port, ks=None):
        self.hostname = hostname or '0.0.0.0'
        self.port = port or DEFAULT_MGMT_PORT
        self.keystore = ks

        self.server = None
        self.node_name = 'mgmt-agent'
        cur_thread = threading.current_thread()
        cur_thread.setName(self.node_name)

    def start(self):
        workers_mgr = WorkersManager(MgmtCommandsProcessor, server_name=self.node_name,
                                            init_params=(self.keystore,))
        fri_server = FriServer(self.hostname, self.port, workers_mgr, self.node_name)
        started = fri_server.start()
        if not started:
            raise Exception('FriServer does not started!')

        self.server = fri_server

    def stop(self):
        if self.server:
            self.server.stop()

