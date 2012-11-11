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
from datetime import datetime

from fabnet.core.operator import Operator
from hash_ranges_table import HashRangesTable
from fabnet.dht_mgmt.fs_mapped_ranges import FSHashRanges
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.utils.logger import logger
from fabnet.dht_mgmt.constants import DS_INITIALIZE, DS_DESTROYING, DS_NORMALWORK, \
            CHECK_HASH_TABLE_TIMEOUT, MIN_HASH, MAX_HASH, DHT_CYCLE_TRY_COUNT, \
            INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT, WAIT_RANGE_TIMEOUT, \
            MONITOR_DHT_RANGES_TIMEOUT, RC_OLD_DATA, \
            MAX_USED_SIZE_PERCENTS, DANGER_USED_SIZE_PERCENTS, \
            PULL_SUBRANGE_SIZE_PERC, CRITICAL_FREE_SPACE_PERCENT
from fabnet.core.constants import RC_OK, NT_SUPERIOR, NT_UPPER, ET_ALERT

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

OPERMAP = { 'GetRangeDataRequest': GetRangeDataRequestOperation,
            'GetRangesTable': GetRangesTableOperation,
            'PutDataBlock': PutDataBlockOperation,
            'GetDataBlock': GetDataBlockOperation,
            'CheckDataBlock': CheckDataBlockOperation,
            'SplitRangeCancel': SplitRangeCancelOperation,
            'SplitRangeRequest': SplitRangeRequestOperation,
            'PullSubrangeRequest': PullSubrangeRequestOperation,
            'UpdateHashRangeTable': UpdateHashRangeTableOperation,
            'CheckHashRangeTable': CheckHashRangeTableOperation,
            'RepairDataBlocks': RepairDataBlocksOperation,
            'GetKeysInfo': GetKeysInfoOperation,
            'PutKeysInfo': PutKeysInfoOperation,
            'ClientGetData': ClientGetOperation,
            'ClientPutData': ClientPutOperation}

class DHTOperator(Operator):
    OPTYPE = 'DHT'

    def __init__(self, self_address, home_dir='/tmp/', certfile=None, is_init_node=False, node_name='unknown'):
        Operator.__init__(self, self_address, home_dir, certfile, is_init_node, node_name)

        self.status = DS_INITIALIZE
        self.ranges_table = HashRangesTable()
        if is_init_node:
            self.ranges_table.append(MIN_HASH, MAX_HASH, self.self_address)

        self.save_path = os.path.join(home_dir, 'dht_range')
        if not os.path.exists(self.save_path):
            os.mkdir(self.save_path)

        self.__dht_range = FSHashRanges.discovery_range(self.save_path)
        self.__split_requests_cache = []
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


    def get_statistic(self):
        stat = Operator.get_statistic(self)
        dht_range = self.get_dht_range()

        dht_i = {}
        dht_i['status'] = self.status
        dht_i['range_start'] = '%040x'% dht_range.get_start()
        dht_i['range_end'] = '%040x'% dht_range.get_end()
        dht_i['range_size'] = dht_range.get_range_size()
        dht_i['replicas_size'] = dht_range.get_replicas_size()
        dht_i['free_size'] = dht_range.get_free_size()
        dht_i['free_size_percents'] = dht_range.get_free_size_percents()
        stat['dht_info'] = dht_i
        return stat


    def on_neigbour_not_respond(self, neighbour_type, neighbour_address):
        if neighbour_type != NT_SUPERIOR:
            return

        for range_obj in self.ranges_table.iter_table():
            if range_obj.node_address == neighbour_address:
                self._move_range(range_obj)
                break


    def _move_range(self, range_obj):
        logger.info('Node %s went from DHT. Updating hash range table on network...'%range_obj.node_address)
        rm_lst = [(range_obj.start, range_obj.end, range_obj.node_address)]
        parameters = {'append': [], 'remove': rm_lst}

        req = FabnetPacketRequest(method='UpdateHashRangeTable', sender=self.self_address, parameters=parameters)
        self.call_network(req)


    def stop(self):
        self.status = DS_DESTROYING
        for range_obj in self.ranges_table.iter_table():
            if range_obj.node_address == self.self_address:
                self._move_range(range_obj)
                break

        Operator.stop(self)

        self.__check_hash_table_thread.stop()
        self.__monitor_dht_ranges.stop()

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

        return max_range


    def __get_next_range_near(self, start, end):
        found_range = self.ranges_table.find(start)
        if found_range and found_range.node_address not in self.__split_requests_cache:
            return found_range

        found_range = self.ranges_table.find(end)
        if found_range and found_range.node_address not in self.__split_requests_cache:
            return found_range

        return None

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

        nochange = False
        curr_start = dht_range.get_start()
        curr_end = dht_range.get_end()

        if dht_range.is_max_range() or self.__split_requests_cache:
            new_range = self.__get_next_max_range()
        else:
            new_range = self.__get_next_range_near(curr_start, curr_end)
            if new_range:
                if (new_range.start != curr_start or new_range.end != curr_end):
                    nochange = True
                if new_range.node_address == self.self_address:
                    self.set_status_to_normalwork()
                    return


        if new_range is None:
            #wait and try again
            if self.__start_dht_try_count == DHT_CYCLE_TRY_COUNT:
                logger.error('Cant initialize node as a part of DHT')
                self.__start_dht_try_count = 0
                return

            logger.info('No ready range for me on network... So, sleep and try again')
            self.__start_dht_try_count += 1
            self.__split_requests_cache = []
            time.sleep(WAIT_RANGE_TIMEOUT)
            return self.start_as_dht_member()

        if nochange:
            new_dht_range = dht_range
        else:
            new_dht_range = FSHashRanges(long(new_range.start + new_range.length()/2+1), long(new_range.end), self.save_path)
            self.update_dht_range(new_dht_range)
            new_dht_range.restore_from_reservation() #try getting new range data from reservation

        self.__split_requests_cache.append(new_range.node_address)

        logger.info('Call SplitRangeRequest to %s'%(new_range.node_address,))
        parameters = { 'start_key': new_dht_range.get_start(), 'end_key': new_dht_range.get_end() }
        req = FabnetPacketRequest(method='SplitRangeRequest', sender=self.self_address, parameters=parameters)
        ret_code, ret_msg = self.call_node(new_range.node_address, req)
        if ret_code != RC_OK:
            logger.error('Cant start SplitRangeRequest operation on node %s. Details: %s'%(new_range.node_address, ret_msg))
            return self.start_as_dht_member()


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

    def check_dht_range(self):
        if self.status == DS_INITIALIZE:
            return

        dht_range = self.get_dht_range()
        if dht_range.get_subranges():
            return

        start = dht_range.get_start()
        end = dht_range.get_end()

        range_obj = self.ranges_table.find(start)
        if not range_obj or range_obj.start != start or range_obj.end != end or range_obj.node_address != self.self_address:
            logger.error('DHT range on this node is not found in ranges_table')
            logger.info('Trying reinit node as DHT member...')
            self.start_as_dht_member()



class CheckLocalHashTableThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = True

    def run(self):
        self.stopped = False
        logger.info('Thread started!')

        while not self.stopped:
            try:
                neighbours = self.operator.get_neighbours(NT_SUPERIOR, self.operator.OPTYPE)
                if not neighbours:
                    time.sleep(INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT)
                    continue

                for neighbour in neighbours:
                    logger.debug('Checking range table at %s'%neighbour)
                    mod_index = self.operator.ranges_table.get_mod_index()
                    params = {'mod_index': mod_index}

                    packet_obj = FabnetPacketRequest(method='CheckHashRangeTable',
                                sender=self.operator.self_address, parameters=params)
                    rcode, rmsg = self.operator.call_node(neighbour, packet_obj)

                    for i in xrange(CHECK_HASH_TABLE_TIMEOUT):
                        if self.stopped:
                            break

                        time.sleep(1)
            except Exception, err:
                logger.error(str(err))


        logger.info('Thread stopped!')

    def stop(self):
        self.stopped = True

class MonitorDHTRanges(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = True

        self.__last_is_start_part = True
        self.__notification_flag = False

    def _check_range_free_size(self):
        dht_range = self.operator.get_dht_range()

        free_percents = dht_range.get_free_size_percents()
        percents = 100 - free_percents
        if percents >= MAX_USED_SIZE_PERCENTS:
            if free_percents < CRITICAL_FREE_SPACE_PERCENT:
                logger.warning('Critical free disk space! Blocking range for write!')
                dht_range.block_for_write()

            logger.warning('Few free size for data range. Trying pull part of range to network')

            if not self._pull_subrange(dht_range):
                self._pull_subrange(dht_range)
        elif percents >= DANGER_USED_SIZE_PERCENTS:
            if self.__notification_flag:
                return
            message = '%s percents'%percents
            params = {'event_type': ET_ALERT, 'event_message': message,\
                      'event_topic': 'HDD usage', 'event_provider': self.operator.self_address}
            packet = FabnetPacketRequest(method='NotifyOperation', parameters=params, sender=self.operator.self_address)
            rcode, rmsg = self.operator.call_network(packet)
            if rcode:
                logger.error('Cant initiate NotifyOperation for ALERT "%s". Details: %s'%(message, rmsg))
            else:
                self.__notification_flag = True
        else:
            self.__notification_flag = False

    def _pull_subrange(self, dht_range, start_part=True):
        split_part = int((dht_range.length() * PULL_SUBRANGE_SIZE_PERC) / 100)
        if self.__last_is_start_part:
            dest_key = dht_range.get_start() - 1
            start_subrange = dht_range.get_start()
            end_subrange = split_part + dht_range.get_start()
        else:
            dest_key = dht_range.get_end() + 1
            start_subrange = dht_range.get_end() - split_part
            end_subrange = dht_range.get_end()

        self.__last_is_start_part = not self.__last_is_start_part

        if MIN_HASH > dest_key > MAX_HASH:
            return False

        k_range = self.operator.ranges_table.find(dest_key)
        if not k_range:
            logger.error('[_pull_subrange] No range found for key=%s in ranges table'%dest_key)
            return False

        pull_subrange, new_dht_range = dht_range.split_range(start_subrange, end_subrange)
        subrange_size = pull_subrange.get_all_related_data_size()
        logger.info('NEW DHT RANGE: [%s-%s]'%(new_dht_range.get_start(), new_dht_range.get_end()))

        try:
            logger.info('Call PullSubrangeRequest to %s'%(k_range.node_address,))
            parameters = { 'start_key': pull_subrange.get_start(), 'end_key': pull_subrange.get_end(), 'subrange_size': subrange_size }
            req = FabnetPacketRequest(method='PullSubrangeRequest', sender=self.operator.self_address, parameters=parameters, sync=True)
            resp = self.operator.call_node(k_range.node_address, req, sync=True)
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
        dht_range = self.operator.get_dht_range()

        for digest, data, file_path in dht_range.iter_reservation():
            logger.info('Processing %s from reservation range'%digest)
            if self._put_data(digest, data):
                logger.debug('data block with key=%s is send from reservation range'%digest)
                os.unlink(file_path)

    def _process_replicas(self):
        dht_range = self.operator.get_dht_range()
        for digest, data, file_path in dht_range.iter_replicas():
            logger.info('Processing replica %s'%digest)
            if self._put_data(digest, data, is_replica=True):
                logger.debug('data block with key=%s is send from replicas range'%digest)
                os.unlink(file_path)


    def _put_data(self, key, data, is_replica=False):
        k_range = self.operator.ranges_table.find(long(key, 16))
        if not k_range:
            logger.debug('No range found for reservation key %s'%key)
            return False

        checksum = hashlib.sha1(data).hexdigest()
        params = {'key': key, 'checksum': checksum, 'is_replica': is_replica, 'carefully_save': True}
        req = FabnetPacketRequest(method='PutDataBlock', sender=self.operator.self_address,\
                parameters=params, binary_data=data, sync=True)

        resp = self.operator.call_node(k_range.node_address, req, sync=True)

        if resp.ret_code not in (RC_OK, RC_OLD_DATA):
            logger.error('PutDataBlock error on %s: %s'%(k_range.node_address, resp.ret_message))
            return False

        return True


    def run(self):
        logger.info('started')
        self.stopped = False
        while not self.stopped:
            try:
                logger.debug('MonitorDHTRanges iteration...')

                self._check_range_free_size()

                self._process_reservation_range()

                self._process_replicas()
            except Exception, err:
                logger.error('[MonitorDHTRanges] %s'% err)
            finally:
                for i in xrange(MONITOR_DHT_RANGES_TIMEOUT):
                    if self.stopped:
                        break
                    time.sleep(1)

        logger.info('stopped')

    def stop(self):
        self.stopped = True


DHTOperator.update_operations_map(OPERMAP)
