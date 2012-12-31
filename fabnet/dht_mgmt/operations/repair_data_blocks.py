#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.repair_data_blocks

@author Konstantin Andrusenko
@date November 10, 2012
"""
import os
import hashlib
import traceback
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE, ET_INFO, ET_ALERT
from fabnet.dht_mgmt.constants import RC_NO_DATA, RC_INVALID_DATA, RC_OLD_DATA
from fabnet.dht_mgmt.data_block import DataBlockHeader
from fabnet.dht_mgmt.key_utils import KeyUtils


class RepairDataBlocksOperation(OperationBase):
    ROLES = [NODE_ROLE]

    def __init__(self, operator):
        OperationBase.__init__(self, operator)
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

    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        self._lock()
        try:
            self.__init_stat(packet.parameters)
            self.__repair_process()
            self._throw_event(ET_INFO, 'RepairDataBlocks statistic', self.__get_stat())
        except Exception, err:
            self._throw_event(ET_ALERT, 'RepairDataBlocks error', err)
            logger.write = logger.debug
            traceback.print_exc(file=logger)
        finally:
            self._unlock()

        return packet

    def __repair_process(self):
        dht_range = self.operator.get_dht_range()

        logger.info('[RepairDataBlocks] Processing local DHT range...')
        for key, header, _ in dht_range.iter_data_blocks(header_only=True):
            self.__process_data_block(key, header, False)
        logger.info('[RepairDataBlocks] Local DHT range is processed!')

        logger.info('[RepairDataBlocks] Processing local replica data...')
        for key, header, _ in dht_range.iter_replicas(foreign_only=False, header_only=True):
            self.__process_data_block(key, header, True)
        logger.info('[RepairDataBlocks] Local replica data is processed!')

    def __process_data_block(self, key, raw_header, is_replica=False):
        self.__processed_local_blocks += 1
        try:
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
        resp = self._init_operation(range_obj.node_address, 'CheckDataBlock', params, sync=True)

        if resp.ret_code in (RC_NO_DATA, RC_INVALID_DATA):
            if local_is_replica:
                data = self.operator.get_dht_range().get_replica(local_key)
            else:
                data = self.operator.get_dht_range().get(local_key)

            params = {'key': check_key, 'is_replica': is_replica, 'carefully_save': True}
            resp = self._init_operation(range_obj.node_address, 'PutDataBlock', params, binary_data=data, sync=True)

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


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        pass

