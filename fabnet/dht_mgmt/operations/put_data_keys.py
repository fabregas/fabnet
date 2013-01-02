#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.get_data_keys

@author Konstantin Andrusenko
@date October 16, 2012
"""
import hashlib

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE, CLIENT_ROLE
from fabnet.dht_mgmt.constants import MIN_REPLICA_COUNT
from fabnet.utils.logger import logger
from fabnet.dht_mgmt.key_utils import KeyUtils


class PutKeysInfoOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
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
        if key is not None:
            self._validate_key(key)
        else:
            key = KeyUtils.generate_key(self.operator.node_name)

        range_obj = self.operator.ranges_table.find(long(key, 16))
        if not range_obj:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message=\
                        '[PutKeysInfoOperation] Internal error: No hash range found for key=%s!'%key)

        return FabnetPacketResponse(ret_parameters={'key_info': (key, range_obj.node_address)})


