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
from fabnet.core.constants import RC_OK, RC_ERROR, RC_PERMISSION_DENIED
from fabnet.dht_mgmt.constants import MIN_REPLICA_COUNT, RC_NO_DATA
from fabnet.utils.logger import oper_logger as logger
from fabnet.dht_mgmt.key_utils import KeyUtils
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

class ClientGetOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    NAME = 'ClientGetData'

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
        checksum = None
        is_replica = False
        for key in keys:
            long_key = self._validate_key(key)

            range_obj = self.operator.find_range(long_key)
            if not range_obj:
                logger.warning('[ClientGetOperation] Internal error: No hash range found for key=%s!'%key)
            else:
                _, _, node_address = range_obj
                resp = self._init_operation(node_address, 'GetDataBlock', {'key': key, 'is_replica': is_replica, \
                        'user_id': packet.session_id}, sync=True)
                if resp.ret_code == RC_PERMISSION_DENIED:
                    return FabnetPacketResponse(ret_code=RC_PERMISSION_DENIED, ret_message=resp.ret_message)
                elif resp.ret_code:
                    logger.warning('[ClientGetOperation] GetDataBlock error from %s: %s'%(node_address, resp.ret_message))
                else:
                    data = resp.binary_data
                    checksum = resp.ret_parameters['checksum']
                    break
            is_replica = True

        if data is None:
            return FabnetPacketResponse(ret_code=RC_NO_DATA, ret_message='No data found!')

        return FabnetPacketResponse(binary_data=data, ret_parameters={'checksum': checksum})


