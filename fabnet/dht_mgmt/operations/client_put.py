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
from fabnet.utils.logger import oper_logger as logger
from fabnet.dht_mgmt.key_utils import KeyUtils
from fabnet.dht_mgmt.data_block import DataBlockHeader
from fabnet.dht_mgmt.fs_mapped_ranges import TmpFile
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE


class ClientPutOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    NAME = 'ClientPutData'

    def init_locals(self):
        self.node_name = self.operator.get_node_name()

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
        if not packet.binary_data:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No binary data found!')

        key = packet.parameters.get('key', None)
        if key is not None:
            self._validate_key(key)

        replica_count = int(packet.parameters.get('replica_count', MIN_REPLICA_COUNT))
        wait_writes_count = int(packet.parameters.get('wait_writes_count', 1))

        if wait_writes_count > (replica_count+1):
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Cant waiting more replicas than saving!')
        if replica_count < MIN_REPLICA_COUNT:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Minimum replica count is equal to %s!'%MIN_REPLICA_COUNT)

        succ_count = 0
        carefully_save = True
        is_replica = False
        errors = []
        local_save = None
        keys = KeyUtils.generate_new_keys(self.node_name, replica_count, prime_key=key)

        tempfile_path = self.operator.get_tempfile()
        tempfile = TmpFile(tempfile_path, packet.binary_data, seek=DataBlockHeader.HEADER_LEN)
        checksum = tempfile.checksum()
        header = DataBlockHeader.pack(keys[0], replica_count, checksum, packet.session_id)
        tempfile.write(header, seek=0)

        for key in keys:
            h_range = self.operator.find_range(key)
            if not h_range:
                logger.info('[ClientPutOperation] Internal error: No hash range found for key=%s!'%key)
            else:
                _, _, node_address = h_range
                params = {'key': key, 'is_replica': is_replica, 'replica_count': replica_count, 'carefully_save': carefully_save}
                if succ_count >= wait_writes_count:
                    self._init_operation(node_address, 'PutDataBlock', params, binary_data=tempfile.chunks())
                else:
                    if self.self_address == node_address and local_save is None:
                        local_save = (key, is_replica)
                    else:
                        resp = self._init_operation(node_address, 'PutDataBlock', params, sync=True, binary_data=tempfile.chunks())
                        if resp.ret_code != RC_OK:
                            logger.error('[ClientPutOperation] PutDataBlock error from %s: %s'%(node_address, resp.ret_message))
                            errors.append('From %s: %s'%(node_address, resp.ret_message))
                        else:
                            succ_count += 1

            is_replica = True

        try:
            if local_save:
                key, is_replica = local_save
                self.operator.put_data_block(key, tempfile.file_path(), is_replica, carefully_save)
                succ_count += 1
        except Exception, err:
            msg = 'Saving data block to local range is failed: %s'%err
            logger.error(msg)
            errors.append(msg)

        if wait_writes_count > succ_count:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Writing data error! Details: \n' + '\n'.join(errors))

        return FabnetPacketResponse(ret_parameters={'key': keys[0], 'checksum': checksum})


