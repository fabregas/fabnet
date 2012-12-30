#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.fri_client
@author Konstantin Andrusenko
@date December 27, 2012

This module contains the implementation of FriClient class.
"""
import socket

import M2Crypto.SSL
from M2Crypto.SSL import Context, Connection

from constants import RC_ERROR, RC_UNEXPECTED, FRI_CLIENT_TIMEOUT, FRI_CLIENT_READ_TIMEOUT

from fri_base import FabnetPacket, FabnetPacketResponse, FriException
from socket_processor import SocketProcessor

class FriClient:
    """class for calling asynchronous operation over FRI protocol"""
    def __init__(self, is_ssl=None, cert=None, session_id=None):
        self.is_ssl = is_ssl
        self.certificate = cert
        self.session_id = session_id

    def __int_call(self, node_address, packet, conn_timeout, read_timeout=None):
        proc = None

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

            if not isinstance(packet, FabnetPacket):
                raise Exception('FRI request packet should be an object of FabnetPacket')

            packet.session_id = self.session_id

            if self.is_ssl:
                context = Context()
                context.set_verify(0, depth = 0)
                sock = Connection(context)
                sock.set_post_connection_check_callback(None)
                sock.set_socket_read_timeout(M2Crypto.SSL.timeout(sec=conn_timeout))
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(conn_timeout)

            sock.connect((hostname, port))

            proc = SocketProcessor(sock, self.certificate)

            proc.send_packet(packet)

            if read_timeout:
                if self.is_ssl:
                    sock.set_socket_read_timeout(M2Crypto.SSL.timeout(sec=read_timeout))
                else:
                    sock.settimeout(read_timeout)

            return proc.get_packet()
        finally:
            if proc:
                proc.close_socket()


    def call(self, node_address, packet, timeout=FRI_CLIENT_TIMEOUT):
        try:
            json_object = self.__int_call(node_address, packet, timeout, FRI_CLIENT_READ_TIMEOUT)

            return json_object.get('ret_code', RC_UNEXPECTED), json_object.get('ret_message', '')
        except Exception, err:
            return RC_ERROR, '[FriClient][%s] %s' % (err.__class__.__name__, err)


    def call_sync(self, node_address, packet, timeout=FRI_CLIENT_TIMEOUT):
        try:
            json_object = self.__int_call(node_address, packet, timeout, FRI_CLIENT_READ_TIMEOUT)

            return FabnetPacketResponse(**json_object)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='[FriClient][%s] %s' % (err.__class__.__name__, err))
