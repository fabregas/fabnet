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

        packet = FabnetPacketRequest(method='PutKeysInfo', parameters={'key': key, 'replica_count': replica_count}, sync=True)
        resp = fri_client.call_sync('%s:%s'%(self.fabnet_hostname, FRI_PORT), packet, FRI_CLIENT_TIMEOUT)
        if resp.ret_code != 0:
            raise Exception('Put keys info error: %s'%resp.ret_message)

        keys_info = resp.ret_parameters['keys_info']
        data = self.security_manager.encrypt(data)
        checksum =  hashlib.sha1(data).hexdigest()

        primary_key = keys_info[0][0]
        for key, is_replica, node_addr in keys_info:
            params = {'primary_key': primary_key, 'key': key, 'checksum': checksum, 'is_replica': is_replica, 'replica_count': replica_count}
            packet = FabnetPacketRequest(method='PutDataBlock', parameters=params, binary_data=data, sync=True)
            resp = fri_client.call_sync(node_addr, packet, FRI_CLIENT_TIMEOUT)
            if resp.ret_code != 0:
                raise Exception('Put data error: %s'%resp.ret_message)

        return primary_key, checksum

    def get(self, primary_key, replica_count=DEFAULT_REPLICA_COUNT):
        network_key = self.security_manager.get_network_key()
        fri_client = FriClient(network_key)

        packet = FabnetPacketRequest(method='GetKeysInfo', parameters={'key': primary_key, 'replica_count': replica_count}, sync=True)
        resp = fri_client.call_sync('%s:%s'%(self.fabnet_hostname, FRI_PORT), packet, FRI_CLIENT_TIMEOUT)
        if resp.ret_code != 0:
            raise Exception('Get keys info error: %s'%resp.ret_message)

        keys_info = resp.ret_parameters['keys_info']
        for key, is_replica, node_addr in keys_info:
            params = {'key': key, 'is_replica': is_replica}
            packet = FabnetPacketRequest(method='GetDataBlock', parameters=params, sync=True)
            resp = fri_client.call_sync(node_addr, packet, FRI_CLIENT_TIMEOUT)

            if resp.ret_code == RC_NO_DATA:
                print 'ERROR: No data found for key %s on node %s'%(key, node_addr) #FIXME
            elif resp.ret_code != 0:
                print 'ERROR: Get data block error for key %s from node %s: %s'%(key, node_addr, resp.ret_message)

            if resp.ret_code == 0:
                exp_checksum = resp.ret_parameters['checksum']
                data = resp.binary_data
                checksum =  hashlib.sha1(data).hexdigest()
                if exp_checksum != checksum:
                    #FIXME: logger.error('Currupted data block on client')
                    print 'ERROR: Currupted data block for key %s from node %s'%(key, node_addr)
                    continue
                data = self.security_manager.decrypt(data)
                return data

        return None


