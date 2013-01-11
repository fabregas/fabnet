#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator.base_operator
@author Konstantin Andrusenko
@date August 22, 2012

This module contains the Operator class implementation
"""
import copy
import threading
import traceback
import time
from datetime import datetime, timedelta
from Queue import Queue

from fabnet.utils.logger import logger
from fabnet.core.config import Config
from fabnet.utils.internal import total_seconds

from fabnet.core.message_container import MessageContainer
from fabnet.core.workers_manager import WorkersManager
from fabnet.core.sessions_manager import SessionsManager
from fabnet.core.operator.async_call_agent import FriAgent
from fabnet.core.operator.neighbours_discovery import NeigboursDiscoveryRoutines
from fabnet.core.constants import MC_SIZE
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.statistic import Statistic
from fabnet.core.constants import RC_OK, RC_ERROR, RC_NOT_MY_NEIGHBOUR, NT_SUPERIOR, NT_UPPER, \
                RC_ALREADY_PROCESSED, RC_MESSAGE_ID_NOT_FOUND, ET_INFO,\
                KEEP_ALIVE_METHOD, KEEP_ALIVE_TRY_COUNT, CHECK_NEIGHBOURS_TIMEOUT,\
                KEEP_ALIVE_MAX_WAIT_TIME, ONE_DIRECT_NEIGHBOURS_COUNT, SO_OPERS_TIME

from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.operations.node_statistic import NodeStatisticOperation
from fabnet.operations.upgrade_node_operation import UpgradeNodeOperation
from fabnet.operations.notify_operation import NotifyOperation
from fabnet.operations.update_node_config import UpdateNodeConfigOperation
from fabnet.operations.get_node_config import GetNodeConfigOperation

from fabnet.operations.constants import NB_NORMAL, NB_MORE, NB_LESS, MNO_REMOVE

OPERLIST = [ManageNeighbour, DiscoveryOperation, TopologyCognition, \
            NodeStatisticOperation, UpgradeNodeOperation, NotifyOperation, \
            GetNodeConfigOperation, UpdateNodeConfigOperation]

NT_MAP = {NT_SUPERIOR: 'Superior', NT_UPPER: 'Upper'}

class OperException(Exception):
    pass

class Operator:
    OPTYPE = 'Base'
    OPERATIONS_LIST = []

    @classmethod
    def update_operations_list(cls, operations_list):
        base_list = copy.copy(cls.OPERATIONS_LIST)
        cls.OPERATIONS_LIST = []
        cls.OPERATIONS_LIST = base_list + operations_list

    def __init__(self, self_address, home_dir='/tmp/', key_storage=None, \
                    is_init_node=False, node_name='unknown-node', config={}):
        self.update_config(config)
        self.msg_container = MessageContainer(MC_SIZE)

        self.__lock = threading.RLock()
        self.self_address = self_address
        self.home_dir = home_dir
        self.node_name = node_name

        self.__neighbours = {NT_SUPERIOR: {}, NT_UPPER: {}}

        self.__upper_keep_alives = {}
        self.__superior_keep_alives = {}

        if key_storage:
            cert = key_storage.get_node_cert()
            ckey = key_storage.get_node_cert_key()
        else:
            cert = ckey = None
        self.fri_client = FriClient(bool(cert), cert, ckey)

        self.__fri_agents_manager = WorkersManager(FriAgent, server_name=node_name, \
                    init_params=(self,))
        self.__fri_agents_manager.start_carefully()
        self.__async_requests = self.__fri_agents_manager.get_queue()

        self.__check_neighbours_thread = CheckNeighboursThread(self)
        self.__check_neighbours_thread.setName('%s-CheckNeighbours'%(node_name,))
        self.__check_neighbours_thread.start()

        self.start_datetime = datetime.now()
        self.is_init_node = is_init_node
        self.__session_manager = SessionsManager(home_dir)
        self.__discovery = NeigboursDiscoveryRoutines(self)
        self.__api_workers_mgr = None
        self.__stat = Statistic()
        init_oper_stat = {}
        for opclass in self.OPERATIONS_LIST:
            init_oper_stat[opclass.get_name()] = 0
        self.__null_oper_stat = init_oper_stat

        self.stopped = threading.Event()

    def get_start_datetime(self):
        return self.start_datetime

    def is_stopped(self):
        return self.stopped.is_set()

    def stop(self):
        try:
            logger.info('stopping operator...')
            self.stopped.set()
            self.__check_neighbours_thread.stop()

            uppers = self.get_neighbours(NT_UPPER)
            superiors = self.get_neighbours(NT_SUPERIOR)

            self.__unbind_neighbours(uppers, NT_SUPERIOR)
            self.__unbind_neighbours(superiors, NT_UPPER)
        except Exception, err:
            logger.error('Operator stopping failed. Details: %s'%err)
        finally:
            try:
                self.stop_inherited()
            except Exception, err:
                logger.error('Inherired operator does not stopped! Details: %s'%err)

            self.__fri_agents_manager.stop()
            self.__check_neighbours_thread.join()
            logger.info('operator is stopped!')

    def stop_inherited(self):
        """This method should be imlemented for destroy
        inherited operator objects"""
        pass

    def on_neigbour_not_respond(self, neighbour_type, neighbour_address):
        """This method can be implemented in inherited class
        for some logic execution on not responsed neighbour"""
        pass

    def __unbind_neighbours(self, neighbours, n_type):
        for neighbour in neighbours:
            parameters = {'neighbour_type': n_type, 'operation': MNO_REMOVE, 'force': True,
                    'node_address': self.self_address}
            req = FabnetPacketRequest(method='ManageNeighbour', sender=self.self_address, parameters=parameters)
            self.call_node(neighbour, req)

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()

    def _process_keep_alive(self, sender):
        if sender not in self.get_neighbours(NT_UPPER):
            rcode = RC_NOT_MY_NEIGHBOUR
        else:
            rcode = RC_OK
            self._lock()
            try:
                self.__upper_keep_alives[sender] = datetime.now()
            finally:
                self._unlock()

        return rcode

    def get_self_address(self):
        return self.self_address

    def get_home_dir(self):
        return self.home_dir

    def get_node_name(self):
        return self.node_name

    def get_type(self):
        return self.OPTYPE

    def has_type(self, optype):
        if optype == self.OPTYPE:
            return True
        return False

    def update_config(self, config):
        Config.update_config(config)

    def get_config(self):
        return Config.get_config_dict()

    def get_config_value(self, config_param):
        return getattr(Config, config_param, None)

    def set_operator_api_workers_manager(self, api_workers_mgr):
        self.__api_workers_mgr = api_workers_mgr

    def on_statisic_request(self):
        ret_params = {}
        uppers = self.get_neighbours(NT_UPPER)
        superiors = self.get_neighbours(NT_SUPERIOR)

        if len(uppers) == ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['uppers_balance'] = NB_NORMAL
        elif len(uppers) > ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['uppers_balance'] = NB_MORE
        else:
            ret_params['uppers_balance'] = NB_LESS

        if len(superiors) == ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['superiors_balance'] = NB_NORMAL
        elif len(superiors) > ONE_DIRECT_NEIGHBOURS_COUNT:
            ret_params['superiors_balance'] = NB_MORE
        else:
            ret_params['superiors_balance'] = NB_LESS

        ret_stat = {'NeighboursInfo': ret_params}
        workers_mgr_list = [self.__fri_agents_manager]
        if self.__api_workers_mgr:
            workers_mgr_list.append(self.__api_workers_mgr)
        for workers_manager in workers_mgr_list:
            w_count, w_busy = workers_manager.get_workers_stat()
            ret_stat['%sWMStat'%workers_manager.get_workers_name()] = \
                                {'workers': w_count, 'busy': w_busy}

        return ret_stat


    def get_statistic(self):
        self._lock()
        try:
            operator_stat = self.on_statisic_request()
            null_op_stat = copy.copy(self.__null_oper_stat)
            stat = self.__stat.dump()
            op_stat = stat.get(SO_OPERS_TIME, {})
            null_op_stat.update(op_stat)
            stat[SO_OPERS_TIME] = null_op_stat
            stat.update(operator_stat)
            return stat
        finally:
            self._unlock()

    def reset_statistic(self):
        self._lock()
        try:
            self.__stat.reset()
        finally:
            self._unlock()

    def update_statistic(self, stat_obj, stat_owner, stat):
        self._lock()
        try:
            self.__stat.update(stat_obj, stat_owner, stat)
        finally:
            self._unlock()

    def register_request(self, message_id, method, sender):
        if method == KEEP_ALIVE_METHOD:
            return self._process_keep_alive(sender)

        inserted = self.msg_container.put_safe(message_id,
                        {'operation': method,
                         'sender': sender,
                         'responses_count': 0,
                         'responses': {},
                         'datetime': datetime.now()})

        if not inserted:
            #this message is already processing/processed
            #logger.debug('packet is already processing/processed: %s'%packet)
            return RC_ALREADY_PROCESSED

        return RC_OK

    def register_callback(self, message_id, from_node=None, params_for_save=None):
        msg_info = self.msg_container.get(message_id)
        if not msg_info:
            return RC_MESSAGE_ID_NOT_FOUND

        self._lock()
        try:
            msg_info['responses_count'] += 1
            op_name = msg_info['operation']
            sender = msg_info['sender']
            if from_node:
                msg_info['responses'][from_node] = params_for_save
        finally:
            self._unlock()

        return op_name, sender

    def wait_response(self, message_id, timeout, response_count=1):
        for i in xrange(timeout*10):
            msg_info = self.msg_container.get(message_id)
            self._lock()
            try:
                if msg_info['responses_count'] >= response_count:
                    break
            finally:
                self._unlock()

            time.sleep(.1)
        else:
            raise OperTimeoutException('Waiting %s response is timeouted'% message_id)


    def update_message(self, message_id, key, value):
        msg_info = self.msg_container.get(message_id)
        self._lock()
        try:
            msg_info[key] = value
        finally:
            self._unlock()


    def get_message_item(self, message_id, key):
        msg_info = self.msg_container.get(message_id)
        self._lock()
        try:
            item = msg_info.get(key, None)
            return copy.copy(item)
        finally:
            self._unlock()


    def set_neighbour(self, neighbour_type, address, optype=None):
        if optype == None:
            optype = self.OPTYPE

        self.__lock.acquire()
        try:
            neighbours = self.__neighbours.get(neighbour_type, None)
            if neighbours is None:
                raise OperException('Neigbour type "%s" is invalid!'%neighbour_type)

            op_n_list = neighbours.get(optype, None)
            if op_n_list is None:
                op_n_list = []
                neighbours[optype] = op_n_list

            if address in op_n_list:
                return

            op_n_list.append(address)
            logger.info('%s neighbour %s with type "%s" is appended'%(NT_MAP[neighbour_type], address, optype))
        finally:
            self.__lock.release()


    def remove_neighbour(self, neighbour_type, address):
        self.__lock.acquire()
        try:
            neighbours = self.__neighbours.get(neighbour_type, None)
            if neighbours is None:
                raise OperException('Neigbour type "%s" is invalid!'%neighbour_type)

            for optype, n_list in neighbours.items():
                try:
                    del n_list[n_list.index(address)]
                    logger.info('%s neighbour %s with type "%s" is removed'%(NT_MAP[neighbour_type], address, optype))
                except ValueError, err:
                    pass

            if address in self.__upper_keep_alives:
                del self.__upper_keep_alives[address]
            if address in self.__superior_keep_alives:
                del self.__superior_keep_alives[address]
        finally:
            self.__lock.release()


    def get_neighbours(self, neighbour_type, optype=None):
        self.__lock.acquire()
        try:
            neighbours = self.__neighbours.get(neighbour_type, None)
            if neighbours is None:
                raise OperException('Neigbour type "%s" is invalid!'%neighbour_type)

            if not neighbours:
                return []

            if optype:
                return neighbours.get(optype)

            ret_list = []
            for types_list in neighbours.values():
                ret_list += types_list

            return ret_list
        finally:
            self.__lock.release()

    def _rebalance_nodes(self):
        self.rebalance_append({'neighbour_type': NT_SUPERIOR})
        self.rebalance_append({'neighbour_type': NT_UPPER})


    def check_neighbours(self):
        ka_packet = FabnetPacketRequest(method=KEEP_ALIVE_METHOD, sender=self.self_address, sync=True)
        superiors = self.get_neighbours(NT_SUPERIOR)

        remove_nodes = []
        for superior in superiors:
            resp = self.fri_client.call_sync(superior, ka_packet)
            cnt = 0
            self._lock()
            try:
                if self.__superior_keep_alives.get(superior, None) is None:
                    self.__superior_keep_alives[superior] = 0

                if resp.ret_code == RC_OK:
                    self.__superior_keep_alives[superior] = 0
                elif resp.ret_code == RC_NOT_MY_NEIGHBOUR:
                    remove_nodes.append((NT_SUPERIOR, superior, False))
                    continue
                else:
                    self.__superior_keep_alives[superior] += 1

                cnt = self.__superior_keep_alives[superior]
            finally:
                self._unlock()

            if cnt == KEEP_ALIVE_TRY_COUNT:
                logger.info('Neighbour %s does not respond. removing it...'%superior)
                remove_nodes.append((NT_SUPERIOR, superior, True))


        #check upper nodes...
        uppers = self.get_neighbours(NT_UPPER)
        self._lock()
        try:
            cur_dt = datetime.now()
            for upper in uppers:
                ka_dt = self.__upper_keep_alives.get(upper, None)
                if ka_dt == None:
                    self.__upper_keep_alives[upper] = datetime.now()
                    continue

                if total_seconds(cur_dt - ka_dt) >= KEEP_ALIVE_MAX_WAIT_TIME:
                    logger.info('No keep alive packets from upper neighbour %s. removing it...'%upper)
                    remove_nodes.append((NT_UPPER, upper, False))
        finally:
            self._unlock()

        for n_type, nodeaddr, is_not_respond in remove_nodes:
            self.remove_neighbour(n_type, nodeaddr)
            if is_not_respond:
                self.on_neigbour_not_respond(n_type, nodeaddr)

        if remove_nodes:
            self._rebalance_nodes()

    def call_node(self, node_address, packet):
        if node_address != self.self_address:
            self.register_request(packet.message_id, packet.method, None)

        return self.__call_operation(node_address, packet)


    def call_network(self, packet, from_address=None):
        packet.sender = None
        packet.is_multicast = True

        if not from_address:
            from_address = self.self_address

        return self.__call_operation(from_address, packet)

    def __call_operation(self, address, packet):
        if packet.is_response:
            packet.from_node = self.self_address
        if hasattr(packet, 'sync') and packet.sync:
            return self.fri_client.call_sync(address, packet)

        self.__async_requests.put((address, packet))

    def async_remote_call(self, node_address, operation, parameters, multicast=False):
        req = FabnetPacketRequest(method=operation, sender=self.self_address, parameters=parameters)
        if multicast:
            self.call_network(req, node_address)
        else:
            self.call_node(node_address, req)

        return req.message_id

    def response_to_sender(self, sender, message_id, ret_code, ret_message, parameters):
        resp = FabnetPacketResponse(message_id=message_id, ret_code=ret_code, \
                                ret_message=ret_message, ret_parameters=parameters)
        self.__call_operation(sender, resp)


    def call_to_neighbours(self, message_id, method, parameters, is_multicast):
        req = FabnetPacketRequest(message_id=message_id, method=method, is_multicast=is_multicast,\
                        sender=self.self_address, parameters=parameters)
        neighbours = self.get_neighbours(NT_SUPERIOR)
        for neighbour in neighbours:
            self.__call_operation(neighbour, req)

    def get_session(self, session_id):
        return self.__session_manager.get(session_id)

    def put_session(self, session_id, role):
        return self.__session_manager.append(session_id, role)

    def discovery_neighbours(self, neighbour):
        packet = FabnetPacketRequest(method='DiscoveryOperation', sender=self.self_address)
        self.call_node(neighbour, packet)

        params = {'event_type': ET_INFO, 'event_topic': 'NodeUp', \
                'event_message': 'Hello, fabnet!', 'event_provider': self.self_address}
        packet = FabnetPacketRequest(method='NotifyOperation', parameters=params)
        self.call_network(packet, neighbour)

    def start_discovery_process(self, node, uppers, superiors):
        self._lock()
        try:
            return self.__discovery.discovery_nodes(node, uppers, superiors)
        finally:
            self._unlock()

    def process_manage_neighbours(self, n_type, operation, node_address, op_type, is_force):
        self._lock()
        try:
            return self.__discovery.process_manage_neighbours(n_type, operation, node_address, op_type, is_force)
        finally:
            self._unlock()

    def callback_manage_neighbours(self, n_type, operation, node_address, op_type, dont_append, dont_remove):
        self._lock()
        try:
            return self.__discovery.callback_manage_neighbours(n_type, operation, node_address, op_type, dont_append, dont_remove)
        finally:
            self._unlock()

    def rebalance_remove(self):
        self._lock()
        try:
            self.__discovery.rebalance_remove()
        finally:
            self._unlock()

    def rebalance_append(self, params):
        self._lock()
        try:
            self.__discovery.rebalance_append(params)
        finally:
            self._unlock()

    def smart_neighbours_rebalance(self, node_address, superior_neighbours, upper_neighbours):
        self._lock()
        try:
            self.__discovery.smart_neighbours_rebalance(node_address, superior_neighbours, upper_neighbours)
        finally:
            self._unlock()



class CheckNeighboursThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = threading.Event()

    def run(self):
        logger.info('Check neighbours thread is started!')

        while not self.stopped.is_set():
            try:
                t0 = datetime.now()

                self.operator.check_neighbours()

                proc_dt = datetime.now() - t0
                logger.debug('CheckNeighbours process time: %s'%proc_dt)
            except Exception, err:
                logger.write = logger.debug
                traceback.print_exc(file=logger)
                logger.error('[CheckNeighboursThread] %s'%err)
            finally:
                wait_seconds = CHECK_NEIGHBOURS_TIMEOUT - proc_dt.seconds
                for i in range(wait_seconds):
                    if self.stopped.is_set():
                        break
                    time.sleep(1)

        logger.info('Check neighbours thread is stopped!')

    def stop(self):
        self.stopped.set()


Operator.update_operations_list(OPERLIST)

