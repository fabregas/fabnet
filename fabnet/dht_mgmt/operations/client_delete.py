#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.client_delete

@author Konstantin Andrusenko
@date June 16, 2013
"""
import hashlib

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.dht_mgmt.key_utils import KeyUtils
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE


class ClientDeleteOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    NAME = 'ClientDeleteData'

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
        key = packet.parameters.get('key', None)
        if key is None:
            raise Exception('key parameter is expected!')

        replica_count = packet.parameters.get('replica_count', None)
        if replica_count is None:
            raise Exception('replica_count parameter is expected!')

        replica_count = int(replica_count)
        self._validate_key(key)

        keys = KeyUtils.generate_new_keys(self.node_name, replica_count, prime_key=key)
        is_replica = False
        for key in keys:
            h_range = self.operator.find_range(key)
            if not h_range:
                return FabnetPacketResponse(ret_code=RC_ERROR, \
                        ret_message='Internal error: No hash range found for key=%s!'%key)
            else:
                _, _, node_address = h_range
                params = {'key': key, 'is_replica': is_replica, \
                            'carefully_delete': True, 'user_id': packet.session_id}

                resp = self._init_operation(node_address, 'DeleteDataBlock', params, sync=True)
                if resp.ret_code != RC_OK:
                    return FabnetPacketResponse(ret_code=resp.ret_code, \
                            ret_message='DeleteDataBlock failed at %s: %s'%(node_address, resp.ret_message))
            is_replica = True

        return FabnetPacketResponse()


