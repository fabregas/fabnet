#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.repair_process

@author Konstantin Andrusenko
@date January 09, 2013
"""
import os
import hashlib

from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.dht_mgmt.constants import RC_NO_DATA, RC_INVALID_DATA, RC_OLD_DATA
from fabnet.dht_mgmt.data_block import DataBlockHeader
from fabnet.dht_mgmt.key_utils import KeyUtils

class RepairProcess:
    def __init__(self, operator):
        self.operator = operator
        self.__invalid_local_blocks = 0
        self.__repaired_foreign_blocks = 0
        self.__failed_repair_foreign_blocks = 0
        self.__processed_local_blocks = 0

    def __init_stat(self, params):
        self.__processes_data_blocks = 0
        self.__invalid_local_blocks = 0
        self.__repaired_foreign_blocks = 0
        self.__failed_repair_foreign_blocks = 0
        self.__processed_local_blocks = 0

        self.__check_range_start = params.get('check_range_start', None)
        self.__check_range_end = params.get('check_range_end', None)
        if self.__check_range_start:
            self.__check_range_start = long(self.__check_range_start, 16)
        if self.__check_range_end:
            self.__check_range_end = long(self.__check_range_end, 16)

    def __get_stat(self):
        return 'processed_local_blocks=%s, invalid_local_blocks=%s, '\
                'repaired_foreign_blocks=%s, failed_repair_foreign_blocks=%s'%\
                (self.__processed_local_blocks, self.__invalid_local_blocks,
                self.__repaired_foreign_blocks, self.__failed_repair_foreign_blocks)

    def _in_check_range(self, key):
        if not self.__check_range_start and not self.__check_range_end:
            return True

        if self.__check_range_start <= long(key, 16) <= self.__check_range_end:
            return True

        return False

    def repair_process(self, params):
        self.__init_stat(params)
        dht_range = self.operator.get_dht_range()

        logger.info('[RepairDataBlocks] Processing local DHT range...')
        for key, data, _ in dht_range.iter_data_blocks():
            self.__process_data_block(key, data, False)
        logger.info('[RepairDataBlocks] Local DHT range is processed!')

        logger.info('[RepairDataBlocks] Processing local replica data...')
        for key, data, _ in dht_range.iter_replicas(foreign_only=False):
            self.__process_data_block(key, data, True)
        logger.info('[RepairDataBlocks] Local replica data is processed!')

        return self.__get_stat()

    def __process_data_block(self, key, raw_data, is_replica=False):
        self.__processed_local_blocks += 1
        try:
            raw_header = raw_data.read(DataBlockHeader.HEADER_LEN)
            primary_key, replica_count, checksum, stored_dt = DataBlockHeader.unpack(raw_header)
            if not is_replica:
                if key != primary_key:
                    raise Exception('Primary key is invalid: %s != %s'%(key, primary_key))

            data_keys = KeyUtils.get_all_keys(primary_key, replica_count)
            if is_replica:
                if key not in data_keys:
                    raise Exception('Replica key is invalid: %s'%key)
        except Exception, err:
            self.__invalid_local_blocks += 1
            logger.error('[RepairDataBlocks] %s'%err)
            return

        if is_replica and self._in_check_range(data_keys[0]):
            self.__check_data_block(key, is_replica,  data_keys[0], checksum, is_replica=False)

        for repl_key in data_keys[1:]:
            if repl_key == key:
                continue

            if self._in_check_range(repl_key):
                self.__check_data_block(key, is_replica, repl_key, checksum, is_replica=True)

    def __validate_key(self, key):
        try:
            if len(key) != 40:
                raise ValueError()
            return long(key, 16)
        except Exception:
            return None

    def __check_data_block(self, local_key, local_is_replica, check_key, checksum, is_replica=False):
        long_key = self.__validate_key(check_key)
        if long_key is None:
            logger.error('[RepairDataBlocks] Invalid data key "%s"'%key)
            self.__invalid_local_blocks += 1

        range_obj = self.operator.ranges_table.find(long_key)
        params = {'key': check_key, 'checksum': checksum, 'is_replica': is_replica}
        req = FabnetPacketRequest(method='CheckDataBlock', sender=self.operator.self_address, sync=True, parameters=params)
        resp = self.operator.call_node(range_obj.node_address, req)

        if resp.ret_code in (RC_NO_DATA, RC_INVALID_DATA):
            logger.info('Invalid data block at %s with key=%s ([%s]%s). Sending valid block...'%\
                    (range_obj.node_address, check_key, resp.ret_code, resp.ret_message))
            data = self.operator.get_dht_range().get(local_key, local_is_replica)

            params = {'key': check_key, 'is_replica': is_replica, 'carefully_save': True}
            req = FabnetPacketRequest(method='PutDataBlock', sender=self.operator.self_address, sync=True, \
                                        parameters=params, binary_data=data)
            resp = self.operator.call_node(range_obj.node_address, req)

            if resp.ret_code == RC_OLD_DATA:
                self.__invalid_local_blocks += 1
                logger.error('Old data block detected with key=%s'%check_key)
            elif resp.ret_code != RC_OK:
                self.__failed_repair_foreign_blocks += 1
                logger.error('PutDataBlock failed on %s. Details: %s'%(range_obj.node_address, resp.ret_message))
            else:
                self.__repaired_foreign_blocks += 1

        elif resp.ret_code != RC_OK:
            self.__failed_repair_foreign_blocks += 1
            logger.error('CheckDataBlock failed on %s. Details: %s'%(range_obj.node_address, resp.ret_message))

