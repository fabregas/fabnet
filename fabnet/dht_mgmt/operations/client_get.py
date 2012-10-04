#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.client_get

@author Konstantin Andrusenko
@date October 3, 2012
"""
import hashlib

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.dht_mgmt.constants import MIN_REPLICA_COUNT
from fabnet.utils.logger import logger
from fabnet.dht_mgmt.data_block import DataBlock
from fabnet.dht_mgmt.key_utils import KeyUtils


class ClientGetOperation(OperationBase):
    def _validate_key(self, key):
        try:
            if len(key) != 40:
                raise ValueError()
            return long(key, 16)
        except Exception:
            raise Exception('Invalid key "%s"'%key)

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.parameters.get('key', None)
        replica_count = packet.parameters.get('replica_count', None)
        if key is None:
            return FabnetPacketResponse(ret_code=RC_ERROR,
                    ret_message='Key is not found in request packet!')

        if replica_count is None:
            return FabnetPacketResponse(ret_code=RC_ERROR,
                    ret_message='Replica count should be passed to GetData operation')

        self._validate_key(key)
        keys = KeyUtils.get_all_keys(key, replica_count)

        data = None
        is_replica = False
        for key in keys:
            long_key = self._validate_key(key)

            range_obj = self.operator.ranges_table.find(long_key)
            if not range_obj:
                logger.warning('[ClientGetOperation] Internal error: No hash range found for key=%s!'%key)
            else:
                resp = self._init_operation(range_obj.node_address, 'GetDataBlock', {'key': key, 'is_replica': is_replica}, sync=True)
                if resp.ret_code:
                    logger.warning('[ClientGetOperation] GetDataBlock error from %s: %s'%(range_obj.node_address, resp.ret_message))
                else:
                    data = resp.binary_data
                    break
            is_replica = True

        if data is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No data found!')

        data_block = DataBlock(data)
        raw_data, raw_checksum = data_block.unpack()

        return FabnetPacketResponse(binary_data=raw_data, ret_parameters={'checksum': raw_checksum})


