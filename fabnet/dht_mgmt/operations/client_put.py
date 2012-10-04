#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.client_put

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


class ClientPutOperation(OperationBase):
    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        data = packet.binary_data
        checksum = packet.parameters.get('checksum', None)
        replica_count = int(packet.parameters.get('replica_count', MIN_REPLICA_COUNT))
        wait_writes_count = int(packet.parameters.get('wait_writes_count', 1))
        if checksum is None:
            return FabnetPacketResponse(ret_code=RC_ERROR,
                    ret_message='Checksum does not found in request packet!')

        if wait_writes_count > replica_count:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Cant waiting more replicas than saving!')
        if replica_count < MIN_REPLICA_COUNT:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Minimum replica count is equal to %s!'%MIN_REPLICA_COUNT)

        data_block = DataBlock(data, checksum)
        data_block.validate()
        succ_count = 0
        is_replica = False
        keys = KeyUtils.generate_new_keys(self.operator.node_name, replica_count)
        data, checksum = data_block.pack(keys[0], replica_count)
        for key in keys:
            range_obj = self.operator.ranges_table.find(long(key, 16))
            if not range_obj:
                logger.debug('[ClientPutOperation] Internal error: No hash range found for key=%s!'%key)
            else:
                params = {'key': key, 'checksum': checksum, 'is_replica': is_replica}
                if succ_count >= wait_writes_count:
                    self._init_operation(range_obj.node_address, 'PutDataBlock', params, binary_data=data)
                else:
                    resp = self._init_operation(range_obj.node_address, 'PutDataBlock', params, sync=True, binary_data=data)
                    if resp.ret_code:
                        logger.debug('[ClientPutOperation] PutDataBlock error from %s: %s'%(range_obj.node_address, resp.ret_message))
                    else:
                        succ_count += 1

            is_replica = True

        if wait_writes_count < succ_count:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Writing data error!')

        return FabnetPacketResponse(ret_parameters={'key': keys[0]})


