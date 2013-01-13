#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.dht_operator

@author Konstantin Andrusenko
@date September 15, 2012
"""
import os
import time
import threading
import hashlib
import random
from datetime import datetime

from fabnet.core.operator import Operator
from hash_ranges_table import HashRange, HashRangesTable
from fabnet.dht_mgmt.fs_mapped_ranges import FSHashRanges
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.utils.logger import logger
from fabnet.core.config import Config
from fabnet.dht_mgmt.constants import DS_INITIALIZE, DS_DESTROYING, DS_NORMALWORK, \
            DEFAULT_DHT_CONFIG, MIN_HASH, MAX_HASH, RC_OLD_DATA, RC_NO_FREE_SPACE
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER, ET_ALERT
from fabnet.dht_mgmt.repair_process import RepairProcess

from fabnet.dht_mgmt.operations.get_range_data_request import GetRangeDataRequestOperation
from fabnet.dht_mgmt.operations.get_ranges_table import GetRangesTableOperation
from fabnet.dht_mgmt.operations.put_data_block import PutDataBlockOperation
from fabnet.dht_mgmt.operations.get_data_block import GetDataBlockOperation
from fabnet.dht_mgmt.operations.check_data_block import CheckDataBlockOperation
from fabnet.dht_mgmt.operations.repair_data_blocks import RepairDataBlocksOperation
from fabnet.dht_mgmt.operations.split_range_cancel import SplitRangeCancelOperation
from fabnet.dht_mgmt.operations.split_range_request import SplitRangeRequestOperation
from fabnet.dht_mgmt.operations.pull_subrange_request import PullSubrangeRequestOperation
from fabnet.dht_mgmt.operations.update_hash_range_table import UpdateHashRangeTableOperation
from fabnet.dht_mgmt.operations.check_hash_range_table import CheckHashRangeTableOperation
from fabnet.dht_mgmt.operations.get_data_keys import GetKeysInfoOperation
from fabnet.dht_mgmt.operations.put_data_keys import PutKeysInfoOperation
from fabnet.dht_mgmt.operations.client_get import ClientGetOperation
from fabnet.dht_mgmt.operations.client_put import ClientPutOperation

OPERLIST = [GetRangeDataRequestOperation, GetRangesTableOperation,
             PutDataBlockOperation, GetDataBlockOperation,
             CheckDataBlockOperation, SplitRangeCancelOperation,
             SplitRangeRequestOperation, PullSubrangeRequestOperation,
             UpdateHashRangeTableOperation, CheckHashRangeTableOperation,
             RepairDataBlocksOperation, GetKeysInfoOperation,
             PutKeysInfoOperation, ClientGetOperation, ClientPutOperation]

class DHTOperator(Operator):
    OPTYPE = 'DHT'

    def __init__(self, self_address, home_dir='/tmp/', key_storage=None, \
                        is_init_node=False, node_name='unknown', config={}):
        Operator.__init__(self, self_address, home_dir, key_storage, \
                                        is_init_node, node_name, config)

        self.status = DS_INITIALIZE
        Config.update_config(DEFAULT_DHT_CONFIG)
        Config.update_config(config)
        self.ranges_table = HashRangesTable()
        if is_init_node:
            self.ranges_table.append(MIN_HASH, MAX_HASH, self.self_address)

        self.save_path = os.path.join(home_dir, 'dht_range')
        if not os.path.exists(self.save_path):
            os.mkdir(self.save_path)

        self.__split_requests_cache = []
        self.__dht_range = FSHashRanges.discovery_range(self.save_path, ret_full=is_init_node)
        self.__start_dht_try_count = 0
        self.__init_dht_thread = None
        if is_init_node:
            self.status = DS_NORMALWORK

        self.__check_hash_table_thread = CheckLocalHashTableThread(self)
        self.__check_hash_table_thread.setName('%s-CheckLocalHashTableThread'%self.node_name)
        self.__check_hash_table_thread.start()

        self.__monitor_dht_ranges = MonitorDHTRanges(self)
        self.__monitor_dht_ranges.setName('%s-MonitorDHTRanges'%self.node_name)
        self.__monitor_dht_ranges.start()

    def get_status(self):
        return self.status

    def on_statisic_request(self):
        stat = Operator.on_statisic_request(self)
        dht_range = self.get_dht_range()

        dht_i = {}
        dht_i['status'] = self.status
        dht_i['range_start'] = '%040x'% dht_range.get_start()
        dht_i['range_end'] = '%040x'% dht_range.get_end()
        dht_i['range_size'] = dht_range.get_range_size()
        dht_i['replicas_size'] = dht_range.get_replicas_size()
        dht_i['free_size'] = dht_range.get_free_size()
        dht_i['free_size_percents'] = dht_range.get_free_size_percents()
        stat['DHTInfo'] = dht_i
        return stat

    def _move_range(self, range_obj):
        logger.info('Node %s (it me) went from DHT. Updating hash range table on network...'%range_obj.node_address)
        rm_lst = [(range_obj.start, range_obj.end, range_obj.node_address)]
        parameters = {'append': [], 'remove': rm_lst}

        req = FabnetPacketRequest(method='UpdateHashRangeTable', sender=self.self_address, parameters=parameters)
        self.call_network(req)

    def _take_range(self, range_obj):
        logger.info('Take node old range. Updating hash range table on network...')
        app_lst = [(range_obj.start, range_obj.end, range_obj.node_address)]
        parameters = {'append': app_lst, 'remove': []}

        req = FabnetPacketRequest(method='UpdateHashRangeTable', sender=self.self_address, parameters=parameters)
        self.call_network(req)


    def stop_inherited(self):
        self.status = DS_DESTROYING
        for range_obj in self.ranges_table.iter_table():
            if range_obj.node_address == self.self_address:
                self._move_range(range_obj)
                break

        self.__check_hash_table_thread.stop()
        self.__monitor_dht_ranges.stop()

        time.sleep(Config.DHT_STOP_TIMEOUT)
        self.__check_hash_table_thread.join()
        self.__monitor_dht_ranges.join()

    def __get_next_max_range(self):
        max_range = None
        for range_obj in self.ranges_table.iter_table():
            if range_obj.node_address in self.__split_requests_cache:
                continue

            if not max_range:
                max_range = range_obj
                continue

            if max_range.length() < range_obj.length():
                max_range = range_obj

        if not max_range:
            return None

        return HashRange(long(max_range.start+max_range.length()/2+1), long(max_range.end), max_range.node_address)

    def __normalize_range_request(self, c_start, c_end, f_range):
        r1 = r2 = None
        if f_range.contain(c_start):
            r1 = HashRange(c_start, f_range.end, f_range.node_address)
        if f_range.contain(c_end):
            r2 = HashRange(f_range.start, c_end, f_range.node_address)

        if r1 and r2:
            if r1.length() < r2.length():
                return r1
            return r2

        if r1:
            return r1
        return r2


    def __get_next_range_near(self, start, end):
        ret_range = None
        found_range = self.ranges_table.find(start)
        if found_range and found_range.node_address not in self.__split_requests_cache:
            ret_range = self.__normalize_range_request(start, end, found_range)

        if found_range and found_range.contain(end):
            return ret_range

        #case when current node range is splited between two other nodes
        found_range = self.ranges_table.find(end)
        if found_range and found_range.node_address not in self.__split_requests_cache:
            ret_range_e = self.__normalize_range_request(start, end, found_range)
            if ret_range_e and ret_range_e.length() > ret_range.length():
                ret_range = ret_range_e

        if not ret_range:
            ret_range = HashRange(start, end, self.self_address)

        return ret_range

    def set_status_to_normalwork(self):
        logger.info('Changing node status to NORMALWORK')
        self.status = DS_NORMALWORK
        self.__split_requests_cache = []
        self.__start_dht_try_count = 0


    def start_as_dht_member(self):
        if self.status == DS_DESTROYING:
            return

        self.status = DS_INITIALIZE
        dht_range = self.get_dht_range()

        curr_start = dht_range.get_start()
        curr_end = dht_range.get_end()

        if dht_range.is_max_range() or self.__split_requests_cache:
            new_range = self.__get_next_max_range()
        else:
            new_range = self.__get_next_range_near(curr_start, curr_end)

        if new_range is None:
            #wait and try again
            if self.__start_dht_try_count == Config.DHT_CYCLE_TRY_COUNT:
                logger.error('Cant initialize node as a part of DHT')
                self.__start_dht_try_count = 0
                return

            logger.info('No ready range for me on network... So, sleep and try again')
            self.__start_dht_try_count += 1
            self.__split_requests_cache = []
            time.sleep(Config.WAIT_RANGE_TIMEOUT)
            return self.start_as_dht_member()

        if (new_range.start == curr_start and new_range.end == curr_end):
            new_dht_range = dht_range
        else:
            new_dht_range = FSHashRanges(long(new_range.start), long(new_range.end), self.save_path)
            self.update_dht_range(new_dht_range)
            new_dht_range.restore_from_reservation() #try getting new range data from reservation

        if new_range.node_address == self.self_address:
            self._take_range(new_range)
            self.set_status_to_normalwork()
            return

        self.__split_requests_cache.append(new_range.node_address)

        logger.info('Call SplitRangeRequest [%040x-%040x] to %s'% \
                (new_dht_range.get_start(), new_dht_range.get_end(), new_range.node_address,))
        parameters = { 'start_key': new_dht_range.get_start(), 'end_key': new_dht_range.get_end() }
        req = FabnetPacketRequest(method='SplitRangeRequest', sender=self.self_address, parameters=parameters)
        self.call_node(new_range.node_address, req)

    def get_dht_range(self):
        self._lock()
        try:
            return self.__dht_range
        finally:
            self._unlock()

    def update_dht_range(self, new_dht_range):
        self._lock()
        old_dht_range = self.__dht_range
        self.__dht_range = new_dht_range
        self._unlock()

        old_dht_range.move_to_reservation()

        dht_range = self.get_dht_range()
        logger.info('New node range: %040x-%040x'%(dht_range.get_start(), dht_range.get_end()))

    def check_dht_range(self, reinit=True):
        if self.status == DS_INITIALIZE:
            return

        dht_range = self.get_dht_range()
        if dht_range.get_subranges():
            return

        start = dht_range.get_start()
        end = dht_range.get_end()

        range_obj = self.ranges_table.find(start)
        if not range_obj:
            range_obj = self.ranges_table.find(end)
        if not range_obj or range_obj.start != start or range_obj.end != end or range_obj.node_address != self.self_address:
            msg = 'Invalid self range!'
            if range_obj:
                msg += ' hash table range - [%040x-%040x]%s... my range - [%040x-%040x]%s'%\
                        (range_obj.start, range_obj.end, range_obj.node_address, start, end, self.self_address)
            else:
                msg += 'Not found in hash table'
            logger.info(msg)

            if (not range_obj) or reinit:
                logger.warning('DHT range on this node is not found in ranges_table')
                if range_obj:
                    logger.info('Self range: %040x-%040x, In hash table: %040x-%040x(%s)'%\
                        (start, end, range_obj.start, range_obj.end, range_obj.node_address))
                logger.info('Trying reinit node as DHT member...')
                self.start_as_dht_member()
            return True

    def check_near_range(self, reinit_dht=False):
        if self.status != DS_NORMALWORK:
            return

        failed_range = self.check_dht_range(reinit=reinit_dht)
        if failed_range:
            return

        self_dht_range = self.get_dht_range()

        if self_dht_range.get_end() != MAX_HASH:
            next_range = self.ranges_table.find(self_dht_range.get_end()+1)
            if not next_range:
                next_exists_range = self.ranges_table.find_next(self_dht_range.get_end()-1)
                if next_exists_range:
                    end = next_exists_range.start-1
                else:
                    end = MAX_HASH
                new_dht_range = self_dht_range.extend(self_dht_range.get_end()+1, end)
                self.update_dht_range(new_dht_range)

                rm_lst = [(self_dht_range.get_start(), self_dht_range.get_end(), self.self_address)]
                append_lst = [(new_dht_range.get_start(), new_dht_range.get_end(), self.self_address)]

                logger.info('Extended range by next neighbours')

                req = FabnetPacketRequest(method='UpdateHashRangeTable', \
                        sender=self.self_address, parameters={'append': append_lst, 'remove': rm_lst})
                self.call_network(req)
                return

        first_range = self.ranges_table.find(MIN_HASH)
        if not first_range:
            first_range = self.ranges_table.get_first()
            if not first_range:
                return
            if first_range.node_address == self.self_address:
                new_dht_range = self_dht_range.extend(MIN_HASH, first_range.start-1)
                self.update_dht_range(new_dht_range)
                rm_lst = [(self_dht_range.get_start(), self_dht_range.get_end(), self.self_address)]
                append_lst = [(new_dht_range.get_start(), new_dht_range.get_end(), self.self_address)]

                logger.info('Extended range by first range')

                req = FabnetPacketRequest(method='UpdateHashRangeTable', \
                        sender=self.self_address, parameters={'append': append_lst, 'remove': rm_lst})
                self.call_network(req)


    def extend_range(self, subrange_size, start_key, end_key):
        dht_range = self.get_dht_range()
        if dht_range.get_subranges():
            raise Exception('Local range is spliited at this time...')

        subrange_size = int(subrange_size)
        estimated_data_size_perc = dht_range.get_estimated_data_percents(subrange_size)

        if estimated_data_size_perc >= Config.MAX_USED_SIZE_PERCENTS:
            raise Exception('Subrange is so big for this node ;(')

        old_range = self.ranges_table.find(start_key)
        if old_range is None:
            raise Exception('No "parent" range found for subrange [%040x-%040x] in distributed ranges table'%(start_key, end_key))

        new_range = dht_range.extend(start_key, end_key)

        if old_range.start < start_key:
            new_foreign_range = (old_range.start, start_key-1, old_range.node_address)
        else:
            new_foreign_range = (end_key+1, old_range.end, old_range.node_address)

        old_foreign_range = (old_range.start, old_range.end, old_range.node_address)
        append_lst = [(new_range.get_start(), new_range.get_end(), self.self_address)]
        append_lst.append(new_foreign_range)
        rm_lst = [(dht_range.get_start(), dht_range.get_end(), self.self_address)]
        rm_lst.append(old_foreign_range)

        self.update_dht_range(new_range)

        req = FabnetPacketRequest(method='UpdateHashRangeTable', \
                    sender=self.self_address,\
                    parameters={'append': append_lst, 'remove': rm_lst})
        self.call_network(req)

    def get_data_block_path(self, key, is_replica):
        return self.get_dht_range().get_path(key, is_replica)

    def get_tempfile(self):
        return self.get_dht_range().tempfile()

    def join_subranges(self):
        self.get_dht_range().join_subranges()

    def put_data_block(self, key, tempfile_path, is_replica, carefully_save):
        self.get_dht_range().put(key, tempfile_path, is_replica, carefully_save)

    def get_subranges(self):
        self.get_dht_range().get_subranges()

    def send_subrange_data(self, node_address):
        dht_range = self.get_dht_range()
        subranges = dht_range.get_subranges()
        if not subranges:
            raise Exception('Range is not splitted!')

        ret_range, new_range = subranges
        try:
            logger.debug('Starting subrange data transfering to %s'% node_address)
            for key, data in ret_range.iter_range():
                params = {'key': key, 'carefully_save': True}
                req = FabnetPacketRequest(method='PutDataBlock', \
                                    sender=self.self_address, binary_data=data, sync=True,
                                    parameters=params)

                resp = self.call_node(node_address, req)
                if resp.ret_code:
                    raise Exception('Init PutDataBlock operation on %s error. Details: %s'%(node_address, resp.ret_message))

            self.update_dht_range(new_range)
        except Exception, err:
            logger.error('send_subrange_data error: %s'%err)
            dht_range.join_subranges()
            raise err

        ret_range._destroy(force=True)
        append_lst = [(ret_range.get_start(), ret_range.get_end(), node_address)]
        append_lst.append((new_range.get_start(), new_range.get_end(), self.self_address))
        rm_lst = [(dht_range.get_start(), dht_range.get_end(), self.self_address)]
        req = FabnetPacketRequest(method='UpdateHashRangeTable', \
                    sender=self.self_address,\
                    parameters={'append': append_lst, 'remove': rm_lst})
        self.call_network(req)

    def find_range(self, key):
        if type(key) in (str, unicode):
            key = long(key, 16)
        range_obj = self.ranges_table.find(key)
        if not range_obj:
            return None
        return range_obj.start, range_obj.end, range_obj.node_address

    def get_ranges_table_status(self):
        c_mod_index = self.ranges_table.get_mod_index()
        c_ranges_count = self.ranges_table.count()
        return c_mod_index, c_ranges_count

    def remove_node_range(self, nodeaddr):
        for range_obj in self.ranges_table.iter_table():
            if range_obj.node_address == nodeaddr:
                self._move_range(range_obj)
                break

    def dump_ranges_table(self):
        return self.ranges_table.dump()

    def restore_ranges_table(self, ranges_table_dump):
        self.ranges_table.load(ranges_table_dump)

    def apply_ranges_table_changes(self, rm_obj_list, ap_obj_list):
        self.ranges_table.apply_changes(rm_obj_list, ap_obj_list)

    def repair_data_process(self, params):
        repair_proc = RepairProcess(self)
        return repair_proc.repair_process(params)

    def split_range(self, start_key, end_key):
        dht_range = self.get_dht_range()

        subranges = dht_range.get_subranges()
        if subranges:
            raise Exception('Already splitting')

        ret_range, new_range = dht_range.split_range(start_key, end_key)
        range_size = ret_range.get_all_related_data_size()

        return range_size

    def accept_foreign_subrange(self, foreign_node, subrange_size):
        dht_range = self.get_dht_range()

        estimated_data_size_perc = dht_range.get_estimated_data_percents(subrange_size)
        if estimated_data_size_perc >= Config.ALLOW_USED_SIZE_PERCENTS:
            logger.info('Requested range is huge for me :( canceling...')
            req = FabnetPacketRequest(method='SplitRangeCancel', sender=self.self_address)
            self.call_node(foreign_node, req)
        else:
            logger.info('Requesting new range data from %s...'%foreign_node)
            req = FabnetPacketRequest(method='GetRangeDataRequest', sender=self.self_address)
            self.call_node(foreign_node, req)




class CheckLocalHashTableThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = threading.Event()

    def run(self):
        logger.info('Thread started!')

        while not self.stopped.is_set():
            try:
                ranges_count = self.operator.ranges_table.count()
                mod_index = self.operator.ranges_table.get_mod_index()
                range_start = self.operator.get_dht_range().get_start()
                range_end = self.operator.get_dht_range().get_end()

                if ranges_count < 2:
                    neighbours = self.operator.get_neighbours(NT_SUPERIOR, self.operator.OPTYPE)
                    if not neighbours:
                        logger.debug('Waiting neighbours...')
                        time.sleep(Config.INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT)
                        continue
                    neighbour = random.choice(neighbours)
                else:
                    neighbour_range = self.operator.ranges_table.find_next(range_start)
                    if not neighbour_range:
                        neighbour_range = self.operator.ranges_table.get_first()
                    neighbour = neighbour_range.node_address

                logger.debug('Checking range table at %s'%neighbour)
                params = {'mod_index': mod_index, 'ranges_count': ranges_count, \
                            'range_start': range_start, 'range_end': range_end}

                packet_obj = FabnetPacketRequest(method='CheckHashRangeTable',
                            sender=self.operator.self_address, parameters=params)
                self.operator.call_node(neighbour, packet_obj)
            except Exception, err:
                logger.error(str(err))

            for i in xrange(Config.CHECK_HASH_TABLE_TIMEOUT):
                if self.stopped.is_set():
                    break
                time.sleep(1)

        logger.info('Thread stopped!')

    def stop(self):
        self.stopped.set()

class MonitorDHTRanges(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = threading.Event()

        self.__last_is_start_part = True
        self.__notification_flag = False
        self.__full_nodes = []

    def _check_range_free_size(self):
        dht_range = self.operator.get_dht_range()

        free_percents = dht_range.get_free_size_percents()
        percents = 100 - free_percents
        if percents >= Config.MAX_USED_SIZE_PERCENTS:
            if free_percents < Config.CRITICAL_FREE_SPACE_PERCENT:
                logger.warning('Critical free disk space! Blocking range for write!')
                dht_range.block_for_write()

            logger.warning('Few free size for data range. Trying pull part of range to network')

            if not self._pull_subrange(dht_range):
                self._pull_subrange(dht_range)
        elif percents >= Config.DANGER_USED_SIZE_PERCENTS:
            if self.__notification_flag:
                return
            message = '%s percents'%percents
            params = {'event_type': ET_ALERT, 'event_message': message,\
                      'event_topic': 'HDD usage', 'event_provider': self.operator.self_address}
            packet = FabnetPacketRequest(method='NotifyOperation', parameters=params, sender=self.operator.self_address)
            self.operator.call_network(packet)
            self.__notification_flag = True
        else:
            self.__notification_flag = False

    def _pull_subrange(self, dht_range):
        split_part = int((dht_range.length() * Config.PULL_SUBRANGE_SIZE_PERC) / 100)
        if self.__last_is_start_part:
            dest_key = dht_range.get_start() - 1
            start_subrange = dht_range.get_start()
            end_subrange = split_part + dht_range.get_start()
        else:
            dest_key = dht_range.get_end() + 1
            start_subrange = dht_range.get_end() - split_part
            end_subrange = dht_range.get_end()

        self.__last_is_start_part = not self.__last_is_start_part

        if dest_key < MIN_HASH:
            logger.info('[_pull_subrange] no range at left...')
            return False

        if dest_key > MAX_HASH:
            logger.info('[_pull_subrange] no range at right...')
            return False

        k_range = self.operator.ranges_table.find(dest_key)
        if not k_range:
            logger.error('[_pull_subrange] No range found for key=%s in ranges table'%dest_key)
            return False

        pull_subrange, new_dht_range = dht_range.split_range(start_subrange, end_subrange)
        subrange_size = pull_subrange.get_all_related_data_size()

        try:
            logger.info('Call PullSubrangeRequest [%040x-%040x] to %s'%(pull_subrange.get_start(), pull_subrange.get_end(), k_range.node_address))
            parameters = { 'start_key': pull_subrange.get_start(), 'end_key': pull_subrange.get_end(), 'subrange_size': subrange_size }
            req = FabnetPacketRequest(method='PullSubrangeRequest', sender=self.operator.self_address, parameters=parameters, sync=True)
            resp = self.operator.call_node(k_range.node_address, req)
            if resp.ret_code != RC_OK:
                raise Exception(resp.ret_message)

            self.operator.update_dht_range(new_dht_range)
            pull_subrange.move_to_reservation()
        except Exception, err:
            logger.error('PullSubrangeRequest operation failed on node %s. Details: %s'%(k_range.node_address, err))
            dht_range.join_subranges()
            return False
        return True

    def _process_reservation_range(self):
        self.__full_nodes = []
        dht_range = self.operator.get_dht_range()

        for digest, data, file_path in dht_range.iter_reservation():
            if self.stopped.is_set():
                break
            logger.info('Processing %s from reservation range'%digest)
            if self._put_data(digest, data):
                logger.debug('data block with key=%s is send from reservation range'%digest)
                os.unlink(file_path)

    def _process_replicas(self):
        self.__full_nodes = []
        dht_range = self.operator.get_dht_range()
        for digest, data, file_path in dht_range.iter_replicas():
            if self.stopped.is_set():
                break
            logger.info('Processing replica %s'%digest)
            if self._put_data(digest, data, is_replica=True):
                logger.debug('data block with key=%s is send from replicas range'%digest)
                os.unlink(file_path)


    def _put_data(self, key, data, is_replica=False):
        k_range = self.operator.ranges_table.find(long(key, 16))
        if not k_range:
            logger.debug('No range found for reservation key %s'%key)
            return False

        if k_range.node_address in self.__full_nodes:
            logger.info('Node %s does not have free space. Skipping put data block...'%k_range.node_address)
            return False

        params = {'key': key, 'is_replica': is_replica, 'carefully_save': True}
        req = FabnetPacketRequest(method='PutDataBlock', sender=self.operator.self_address,\
                parameters=params, binary_data=data, sync=True)

        resp = self.operator.call_node(k_range.node_address, req)

        if resp.ret_code == RC_NO_FREE_SPACE:
            self.__full_nodes.append(k_range.node_address)

        if resp.ret_code not in (RC_OK, RC_OLD_DATA):
            logger.error('PutDataBlock error on %s: %s'%(k_range.node_address, resp.ret_message))
            return False

        return True


    def run(self):
        logger.info('started')
        while True:
            for i in xrange(Config.MONITOR_DHT_RANGES_TIMEOUT):
                if self.stopped.is_set():
                    break
                time.sleep(1)

            if self.stopped.is_set():
                break

            try:
                logger.debug('MonitorDHTRanges iteration...')

                self._check_range_free_size()
                if self.stopped.is_set():
                    break

                self._process_reservation_range()
                if self.stopped.is_set():
                    break

                self._process_replicas()
                if self.stopped.is_set():
                    break
            except Exception, err:
                logger.error('[MonitorDHTRanges] %s'% err)

        logger.info('stopped')

    def stop(self):
        self.stopped.set()


DHTOperator.update_operations_list(OPERLIST)
