#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator
@author Konstantin Andrusenko
@date August 22, 2012

This module contains the Operator class implementation
"""
import copy
import threading
import traceback
import time
import random
from datetime import datetime, timedelta
from Queue import Queue

from fabnet.utils.logger import logger
from fabnet.utils.internal import total_seconds

from fabnet.core.operation_base import OperationBase
from fabnet.core.message_container import MessageContainer
from fabnet.core.agents_manager import FriAgentsManager
from fabnet.core.constants import MC_SIZE, RQ_SIZE
from fabnet.core.fri_base import FriClient, FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.config import Config
from fabnet.core.constants import RC_OK, RC_ERROR, RC_NOT_MY_NEIGHBOUR, NT_SUPERIOR, NT_UPPER, \
                KEEP_ALIVE_METHOD, KEEP_ALIVE_TRY_COUNT, CHECK_NEIGHBOURS_TIMEOUT,\
                KEEP_ALIVE_MAX_WAIT_TIME, ONE_DIRECT_NEIGHBOURS_COUNT, WAIT_SYNC_OPERATION_TIMEOUT

from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.operations.node_statistic import NodeStatisticOperation
from fabnet.operations.upgrade_node_operation import UpgradeNodeOperation
from fabnet.operations.notify_operation import NotifyOperation
from fabnet.operations.update_node_config import UpdateNodeConfigOperation
from fabnet.operations.get_node_config import GetNodeConfigOperation

from fabnet.operations.constants import NB_NORMAL, NB_MORE, NB_LESS, MNO_REMOVE

NEED_STAT = True

NT_MAP = {NT_SUPERIOR: 'Superior', NT_UPPER: 'Upper'}

class OperationStat:
    def __init__(self):
        self.call_cnt = 0
        self.proc_time = timedelta()

    def update(self, proc_time):
        self.call_cnt += 1
        self.proc_time += proc_time

    def dump(self):
        if self.call_cnt:
            avg = self.proc_time/self.call_cnt
        else:
            avg = 0

        return {'call_cnt': self.call_cnt,
                'avg_proc_time': str(avg)}


class OperException(Exception):
    pass

class OperTimeoutException(OperException):
    pass


OPERMAP =  {'ManageNeighbour': ManageNeighbour,
            'DiscoveryOperation': DiscoveryOperation,
            'TopologyCognition': TopologyCognition,
            'NodeStatistic': NodeStatisticOperation,
            'UpgradeNode': UpgradeNodeOperation,
            'NotifyOperation': NotifyOperation,
            'GetNodeConfig': GetNodeConfigOperation,
            'UpdateNodeConfig': UpdateNodeConfigOperation}


class Operator:
    OPTYPE = 'Base'
    OPERATIONS_MAP = {}

    @classmethod
    def update_operations_map(cls, operations_map):
        base_map = copy.copy(cls.OPERATIONS_MAP)
        cls.OPERATIONS_MAP = {}
        cls.OPERATIONS_MAP.update(base_map)
        cls.OPERATIONS_MAP.update(operations_map)

    def __init__(self, self_address, home_dir='/tmp/', key_storage=None, is_init_node=False, node_name='unknown-node'):
        self.__operations = {}
        if NEED_STAT:
            self.__stat = {}
        else:
            self.__stat = None

        self.msg_container = MessageContainer(MC_SIZE)

        self.__lock = threading.RLock()
        self.self_address = self_address
        self.home_dir = home_dir
        self.node_name = node_name
        self.server = None

        self.__neighbours = {NT_SUPERIOR: {}, NT_UPPER: {}}

        if key_storage:
            cert = key_storage.get_node_cert()
            ckey = key_storage.get_node_cert_key()
        else:
            cert = ckey = None
        self.fri_client = FriClient(bool(cert), cert, ckey)

        self.__fri_requests_queue = Queue()
        self.__fri_responses_queue = MessageContainer(RQ_SIZE)
        self.__fri_agents_manager = FriAgentsManager(self.__fri_requests_queue, \
                                        self.__fri_responses_queue, key_storage, prefix=node_name)
        self.__fri_agents_manager.setName('%s-FriAgentsManager'%(node_name,))
        self.__fri_agents_manager.start()

        self.__check_neighbours_thread = CheckNeighboursThread(self)
        self.__check_neighbours_thread.setName('%s-CheckNeighbours'%(node_name,))
        self.__check_neighbours_thread.start()

        self.__upper_keep_alives = {}
        self.__superior_keep_alives = {}

        self.start_datetime = datetime.now()
        self.is_init_node = is_init_node

        for op_name, op_class in self.OPERATIONS_MAP.items():
            self.register_operation(op_name, op_class)

        self.stopped = False

    def __unbind_neighbours(self, neighbours, n_type):
        for neighbour in neighbours:
            parameters = { 'neighbour_type': n_type, 'operation': MNO_REMOVE, 'force': True,
                    'node_address': self.self_address }
            req = FabnetPacketRequest(method='ManageNeighbour', sender=self.self_address, parameters=parameters)
            self.call_node(neighbour, req)

    def __call_operation(self, address, packet):
        is_sync = getattr(packet, 'sync', False)
        if is_sync:
            event = threading.Event()
        else:
            event = None

        self.__fri_requests_queue.put((address, packet, event, self.callback))

        if is_sync:
            ok = event.wait(WAIT_SYNC_OPERATION_TIMEOUT)
            if (ok is not None) and (not ok):
                raise OperException('Operation %s timeouted on %s'%(packet.method, address))

            ret_packet = self.__fri_responses_queue.get(packet.message_id, remove=True)
            return ret_packet

    def stop(self):
        try:
            self.stopped = True
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

    def stop_inherited(self):
        """This method should be imlemented for destroy
        inherited operator objects"""
        pass

    def on_network_notify(self, notify_type, notify_provider, notify_topic, message):
        """This method should be imlemented for some actions
            on received network nofitications
        """
        pass

    def has_type(self, optype):
        if optype == self.OPTYPE:
            return True
        return False

    def get_statistic(self):
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

        count, busy = self.server.workers_stat()
        ret_params['workers_count'] = count
        ret_params['workers_busy'] = busy

        count, busy = self.__fri_agents_manager.get_workers_stat()
        ret_params['agents_count'] = count
        ret_params['agents_busy'] = busy

        methods_stat = {}
        self._lock()
        try:
            for op, op_stat in self.__stat.items():
                methods_stat[op] = op_stat.dump()
        finally:
            self._unlock()

        ret_params['methods_stat'] = methods_stat

        return ret_params

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()

    def set_node_name(self, node_name):
        self.node_name = node_name

    def set_server(self, server):
        self.server = server

    def get_operation_instance(self, method_name):
        operation_obj = self.__operations.get(method_name, None)
        if operation_obj is None:
            raise OperException('Method "%s" does not implemented!'%method_name)

        return operation_obj

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

    def on_neigbour_not_respond(self, neighbour_type, neighbour_address):
        """This method can be implemented in inherited class
        for some logic execution on not responsed neighbour"""
        pass

    def register_operation(self, op_name, op_class):
        if not issubclass(op_class, OperationBase):
            raise OperException('Class %s does not inherit OperationBase class'%op_class)

        self.__operations[op_name] = op_class(self)
        if self.__stat is not None:
            self.__stat[op_name] = OperationStat()

    def call_node(self, node_address, packet):
        if node_address != self.self_address:
            self.msg_container.put(packet.message_id,
                            {'operation': packet.method,
                                'sender': None,
                                'responses_count': 0,
                                'datetime': datetime.now()})

        return self.__call_operation(node_address, packet)


    def call_network(self, packet, from_address=None):
        packet.sender = None
        packet.is_multicast = True

        if not from_address:
            from_address = self.self_address

        return self.__call_operation(from_address, packet)


    def _process_keep_alive(self, packet):
        if packet.sender not in self.get_neighbours(NT_UPPER):
            rcode = RC_NOT_MY_NEIGHBOUR
        else:
            rcode = RC_OK
            self.__lock.acquire()
            try:
                self.__upper_keep_alives[packet.sender] = datetime.now()
            finally:
                self.__lock.release()

        return FabnetPacketResponse(from_node=self.self_address,
                        message_id=packet.message_id, ret_code=rcode)

    def _rebalance_nodes(self):
        operation_obj = self.__operations.get('ManageNeighbour', None)
        if not operation_obj:
            logger.error('ManageNeighbour does not found. Cant rebalance node!')
            return

        operation_obj.rebalance_append({'neighbour_type': NT_SUPERIOR})
        operation_obj.rebalance_append({'neighbour_type': NT_UPPER})


    def check_neighbours(self):
        ka_packet = FabnetPacketRequest(method=KEEP_ALIVE_METHOD, sender=self.self_address, sync=True)
        superiors = self.get_neighbours(NT_SUPERIOR)

        remove_nodes = []
        for superior in superiors:
            resp = self.fri_client.call_sync(superior, ka_packet)
            cnt = 0
            self.__lock.acquire()
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
                self.__lock.release()

            if cnt == KEEP_ALIVE_TRY_COUNT:
                logger.info('Neighbour %s does not respond. removing it...'%superior)
                remove_nodes.append((NT_SUPERIOR, superior, True))


        #check upper nodes...
        uppers = self.get_neighbours(NT_UPPER)
        self.__lock.acquire()
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
            self.__lock.release()

        for n_type, nodeaddr, is_not_respond in remove_nodes:
            self.remove_neighbour(n_type, nodeaddr)
            if is_not_respond:
                self.on_neigbour_not_respond(n_type, nodeaddr)

        if remove_nodes:
            self._rebalance_nodes()




    def process(self, packet, role=None):
        """process request fabnet packet
        @param packet - object of FabnetPacketRequest class
        @param role - requestor role (None for disable auth)
        """
        t0 = None
        try:
            if packet.method == KEEP_ALIVE_METHOD:
                return self._process_keep_alive(packet)

            inserted = self.msg_container.put_safe(packet.message_id,
                            {'operation': packet.method,
                             'sender': packet.sender,
                             'responses_count': 0,
                             'datetime': datetime.now()})

            if not inserted:
                #this message is already processing/processed
                #logger.debug('packet is already processing/processed: %s'%packet)
                return

            if self.__stat is not None:
                t0 = datetime.now()

            operation_obj = self.__operations.get(packet.method, None)
            if operation_obj is None:
                if packet.is_multicast:
                    self._send_to_neighbours(packet)
                    return
                else:
                    raise OperException('Method "%s" does not implemented!'%packet.method)

            operation_obj.check_role(role)

            logger.debug('processing packet %s'%packet)

            message_id = packet.message_id
            n_packet = operation_obj.before_resend(packet)
            if n_packet:
                n_packet = copy.copy(n_packet)
                n_packet.message_id = message_id
                n_packet.sync = False
                self._send_to_neighbours(n_packet)

            s_packet = operation_obj.process(packet)
            if s_packet:
                s_packet.message_id = packet.message_id
                s_packet.from_node = self.self_address

            return s_packet
        except Exception, err:
            err_packet = FabnetPacketResponse(from_node=self.self_address,
                            message_id=packet.message_id,
                            ret_code=1, ret_message= '[OpPROC] %s'%err)
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            logger.error('[Operator.process] %s'%err)
            return err_packet
        finally:
            if self.__stat is not None and t0:
                self._lock()
                try:
                    if packet.method in self.__stat:
                        self.__stat[packet.method].update(datetime.now()-t0)
                finally:
                    self._unlock()

    def after_process(self, packet, ret_packet):
        """process some logic after response send"""
        if packet.method == KEEP_ALIVE_METHOD:
            return

        msg_info = self.msg_container.get(packet.message_id)
        self.__lock.acquire()
        try:
            if not msg_info:
                raise OperException('Message with ID %s does not found in after_process! {Packet: %s}'%(packet.message_id, packet))

            operation = msg_info['operation']
        finally:
            self.__lock.release()

        operation_obj = self.__operations.get(operation, None)
        if operation_obj is None:
            return

        try:
            operation_obj.after_process(packet, ret_packet)
        except Exception, err:
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            logger.error('%s after_process routine failed. Details: %s'%(operation, err))


    def callback(self, packet):
        """process callback fabnet packet
        @param packet - object of FabnetPacketResponse class
        """
        msg_info = self.msg_container.get(packet.message_id)
        self.__lock.acquire()
        try:
            if not msg_info:
                raise OperException('Message with ID %s does not found! {Packet: %s}'%(packet.message_id, packet))

            msg_info['responses_count'] += 1
            operation = msg_info['operation']
            sender = msg_info['sender']
        finally:
            self.__lock.release()

        operation_obj = self.__operations.get(operation, None)

        if operation_obj is None:
            if sender:
                return self.send_to_sender(sender, packet)

        s_packet = None
        try:
            s_packet = operation_obj.callback(packet, sender)
        except Exception, err:
            logger.error('%s callback failed. Details: %s'%(operation, err))

        if s_packet:
            self.send_to_sender(sender, s_packet)

    def wait_response(self, message_id, timeout, response_count=1):
        for i in xrange(timeout*10):
            msg_info = self.msg_container.get(message_id)
            self.__lock.acquire()
            try:
                if msg_info['responses_count'] >= response_count:
                    break
            finally:
                self.__lock.release()

            time.sleep(.1)
        else:
            raise OperTimeoutException('Waiting %s response is timeouted'% message_id)


    def update_message(self, message_id, key, value):
        msg_info = self.msg_container.get(message_id)
        self.__lock.acquire()
        try:
            msg_info[key] = value
        finally:
            self.__lock.release()


    def get_message_item(self, message_id, key):
        msg_info = self.msg_container.get(message_id)
        self.__lock.acquire()
        try:
            item = msg_info.get(key, None)
            return copy.copy(item)
        finally:
            self.__lock.release()


    def _send_to_neighbours(self, packet):
        packet.sender = self.self_address
        neighbours = self.get_neighbours(NT_SUPERIOR)

        for neighbour in neighbours:
            self.__call_operation(neighbour, packet)

    def send_to_sender(self, sender, packet):
        if sender is None:
            self.callback(packet)
            return

        self.__call_operation(sender, packet)




class CheckNeighboursThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = True

    def run(self):
        self.stopped = False
        logger.info('Check neighbours thread is started!')

        while not self.stopped:
            try:
                t0 = datetime.now()

                self.operator.check_neighbours()

                proc_dt = datetime.now() - t0
            except Exception, err:
                logger.write = logger.debug
                traceback.print_exc(file=logger)
                logger.error('[CheckNeighboursThread] %s'%err)
            finally:
                wait_seconds = CHECK_NEIGHBOURS_TIMEOUT - proc_dt.seconds
                for i in range(wait_seconds):
                    if self.stopped:
                        break
                    time.sleep(1)

        logger.info('Check neighbours thread is stopped!')

    def stop(self):
        self.stopped = True



Operator.update_operations_map(OPERMAP)
