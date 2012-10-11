#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package client.nibbler
@author Konstantin Andrusenko
@date October 12, 2012

This module contains the implementation of gateway API for talking with fabnet
"""
import hashlib
from fabnet.core.fri_base import FriClient, FabnetPacketRequest
from client.constants import DEFAULT_REPLICA_COUNT, FRI_PORT, FRI_CLIENT_TIMEOUT, \
                            RC_NO_DATA


class FabnetGateway:
    def __init__(self, fabnet_hostname, security_manager):
        self.fabnet_hostname = fabnet_hostname
        self.security_manager = security_manager

    def put(self, data, key=None, replica_count=DEFAULT_REPLICA_COUNT):
        network_key = self.security_manager.get_network_key()
        fri_client = FriClient(network_key)

        data = self.security_manager.encrypt(data)

        checksum =  hashlib.sha1(data).hexdigest()

        params = {'key': key, 'replica_count': replica_count, 'checksum': checksum, 'wait_writes_count': 1}
        packet = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=data, sync=True)

        resp = fri_client.call_sync('%s:%s'%(self.fabnet_hostname, FRI_PORT), packet, FRI_CLIENT_TIMEOUT)
        if resp.ret_code != 0:
            raise Exception('Put data block error: %s'%resp.ret_message)

        return resp.ret_parameters['key'], checksum

    def get(self, key, replica_count=DEFAULT_REPLICA_COUNT):
        network_key = self.security_manager.get_network_key()
        fri_client = FriClient(network_key)

        params = {'key': key, 'replica_count': replica_count}
        packet = FabnetPacketRequest(method='ClientGetData', parameters=params, sync=True)

        resp = fri_client.call_sync('%s:%s'%(self.fabnet_hostname, FRI_PORT), packet, FRI_CLIENT_TIMEOUT)
        if resp.ret_code == RC_NO_DATA:
            return None

        if resp.ret_code != 0:
            raise Exception('Get data block error: %s'%resp.ret_message)

        exp_checksum = resp.ret_parameters['checksum']
        data = resp.binary_data
        checksum =  hashlib.sha1(data).hexdigest()

        if exp_checksum != checksum:
            raise Exception('Currupted data block on client')

        data = self.security_manager.decrypt(data)

        return data
