#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.fri_base
@author Konstantin Andrusenko
@date August 20, 2012

This module contains the implementation of FabnetPacketRequest, FabnetPacketResponse, FriClient classes.
"""
import ssl
import uuid
import socket
import struct
import zlib
import json

from constants import RC_OK, RC_ERROR, RC_UNEXPECTED, \
                BUF_SIZE, FRI_CLIENT_TIMEOUT, \
                FRI_PROTOCOL_IDENTIFIER, FRI_PACKET_INFO_LEN


class FriException(Exception):
    pass


class FriBinaryProcessor:
    @classmethod
    def get_expected_len(cls, data):
        p_info = data[:FRI_PACKET_INFO_LEN]
        if len(p_info) != FRI_PACKET_INFO_LEN:
            return None

        try:
            prot, packet_len, header_len = struct.unpack('<4sqq', p_info)
        except Exception, err:
            raise FriException('Invalid FRI packet! Packet information is corrupted: %s'%err)

        if prot != FRI_PROTOCOL_IDENTIFIER:
            raise FriException('Invalid FRI packet! Protocol is mismatch')

        return packet_len, header_len


    @classmethod
    def from_binary(cls, data, packet_len, header_len):
        if len(data) != int(packet_len):
            raise FriException('Invalid FRI packet! Packet length %s is differ to expected %s'%(len(data), packet_len))

        header = data[FRI_PACKET_INFO_LEN:FRI_PACKET_INFO_LEN+header_len]
        if len(header) != int(header_len):
            raise FriException('Invalid FRI packet! Header length %s is differ to expected %s'%(len(header), header_len))

        try:
            json_header = json.loads(header)
        except Exception, err:
            raise FriException('Invalid FRI packet! Header is corrupted: %s'%err)

        bin_data = data[header_len+FRI_PACKET_INFO_LEN:]
        if bin_data:
            bin_data = zlib.decompress(bin_data)

        return json_header, bin_data

    @classmethod
    def to_binary(cls, header_obj, bin_data=''):
        try:
            header = json.dumps(header_obj)
        except Exception, err:
            raise FriException('Cant form FRI packet! Header is corrupted: %s'%err)

        if bin_data:
            bin_data = zlib.compress(bin_data)

        h_len = len(header)
        packet_data = header + bin_data
        p_len = len(packet_data) + FRI_PACKET_INFO_LEN
        p_info = struct.pack('<4sqq', FRI_PROTOCOL_IDENTIFIER, p_len, h_len)

        return p_info + packet_data


class FabnetPacketRequest:
    def __init__(self, **packet):
        self.message_id = packet.get('message_id', None)
        self.sync = packet.get('sync', False)
        if not self.message_id:
            self.message_id = str(uuid.uuid1())
        self.method = packet.get('method', None)
        self.sender = packet.get('sender', None)
        self.parameters = packet.get('parameters', {})
        self.binary_data = packet.get('binary_data', '')

        self.validate()

    def copy(self):
        return FabnetPacketRequest(**self.to_dict())

    def validate(self):
        if self.message_id is None:
            raise FriException('Invalid packet: message_id does not exists')

        if self.method is None:
            raise FriException('Invalid packet: method does not exists')


    def dump(self):
        header_json = self.to_dict()
        data = FriBinaryProcessor.to_binary(header_json, self.binary_data)
        return data

    def to_dict(self):
        ret_dict = {'message_id': self.message_id, \
                'method': self.method, \
                'sender': self.sender, \
                'sync': self.sync}

        if self.parameters:
            ret_dict['parameters'] = self.parameters
        return ret_dict

    def __str__(self):
        return str(self.__repr__())

    def __repr__(self):
        return '{%s}[%s] %s %s'%(self.message_id, self.sender, self.method, str(self.parameters))


class FabnetPacketResponse:
    def __init__(self, **packet):
        self.message_id = packet.get('message_id', None)
        self.ret_code = packet.get('ret_code', RC_OK)
        self.ret_message = packet.get('ret_message', '')
        self.ret_parameters = packet.get('ret_parameters', {})
        self.from_node = packet.get('from_node', None)
        self.binary_data = packet.get('binary_data', '')

    def dump(self):
        header_json = self.to_dict()
        data = FriBinaryProcessor.to_binary(header_json, self.binary_data)
        return data

    def to_dict(self):
        ret_dict = {'ret_code': self.ret_code,
                'ret_message': self.ret_message}

        if self.message_id:
            ret_dict['message_id'] = self.message_id
        if self.ret_parameters:
            ret_dict['ret_parameters'] = self.ret_parameters
        if self.from_node:
            ret_dict['from_node'] = self.from_node

        return ret_dict


    def __str__(self):
        return str(self.__repr__())

    def __repr__(self):
        return '{%s}[%s] %s %s %s'%(self.message_id, self.from_node,
                    self.ret_code, self.ret_message, str(self.ret_parameters)[:100])




#------------- FRI client class ----------------------------------------------

class FriClient:
    """class for calling asynchronous operation over FRI protocol"""
    def __init__(self, certfile=None):
        self.certfile = certfile

    def __int_call(self, node_address, packet, timeout):
        sock = None

        try:
            address = node_address.split(':')
            if len(address) != 2:
                raise FriException('Node address %s is invalid! ' \
                            'Address should be in format <hostname>:<port>'%node_address)
            hostname = address[0]
            try:
                port = int(address[1])
                if 0 > port > 65535:
                    raise ValueError()
            except ValueError:
                raise FriException('Node address %s is invalid! ' \
                            'Port should be integer in range 0...65535'%node_address)


            if type(packet) == str:
                data = packet
            else:
                data = packet.dump()


            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.certfile:
                sock = ssl.wrap_socket(sock, ca_certs=self.certfile,
                                    cert_reqs=ssl.CERT_REQUIRED)
            sock.settimeout(timeout)

            sock.connect((hostname, port))

            sock.settimeout(None)

            sock.sendall(data)
            #sock.shutdown(socket.SHUT_WR)

            data = ''
            exp_len = None
            header_len = 0
            while True:
                received = sock.recv(BUF_SIZE)

                if not received:
                    break

                data += received

                if exp_len is None:
                    exp_len, header_len = FriBinaryProcessor.get_expected_len(data)
                if exp_len and len(data) >= exp_len:
                    break

            if not data:
                raise FriException('empty data block')

            header, bin_data = FriBinaryProcessor.from_binary(data, exp_len, header_len)
            header['binary_data'] = bin_data
            return header
        finally:
            if sock:
                sock.close()

    def call(self, node_address, packet, timeout=FRI_CLIENT_TIMEOUT):
        try:
            json_object = self.__int_call(node_address, packet, timeout)

            return json_object.get('ret_code', RC_UNEXPECTED), json_object.get('ret_message', '')
        except Exception, err:
            return RC_ERROR, '[FriClient] %s' % err


    def call_sync(self, node_address, packet, timeout=FRI_CLIENT_TIMEOUT):
        try:
            json_object = self.__int_call(node_address, packet, timeout)

            return FabnetPacketResponse(**json_object)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='[FriClient] %s'%err)


