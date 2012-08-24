#!/usr/bin/python
"""
Copyright (C) 2011 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.fri_base
@author Konstantin Andrusenko
@date August 20, 2012

This module contains the implementation of FriServer and FriClient classes.
"""

import socket
import threading
import time
from Queue import Queue

import json

from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, RC_UNEXPECTED, STOP_THREAD_EVENT, \
                        S_ERROR, S_PENDING, S_INWORK, \
                        BUF_SIZE, KEEP_ALIVE_PACKET

class FriException(Exception):
    pass


class FabnetPacketRequest:
    def __init__(self, **packet):
        self.message_id = packet.get('message_id', None)
        self.method = packet.get('method', None)
        self.sender = packet.get('sender', None)
        self.parameters = packet.get('parameters', {})

        self.validate()

    def copy(self):
        return FabnetPacketRequest(**self.to_dict())

    def validate(self):
        #if self.message_id is None:
        #    raise FriException('Invalid packet: message_id does not exists')

        #if self.sender is None:
        #    raise FriException('Invalid packet: sender does not exists')

        if self.method is None:
            raise FriException('Invalid packet: method does not exists')



    def to_dict(self):
        return {'message_id': self.message_id, \
                'method': self.method, \
                'sender': self.sender, \
                'parameters': self.parameters }

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return str(self.to_dict())


class FabnetPacketResponse:
    def __init__(self, **packet):
        self.message_id = packet.get('message_id', None)
        self.ret_code = packet.get('ret_code', RC_OK)
        self.ret_message = packet.get('ret_message', '')
        self.ret_parameters = packet.get('ret_parameters', {})

    def to_dict(self):
        return {'message_id': self.message_id,
                'ret_code': self.ret_code,
                'ret_message': self.ret_message,
                'ret_parameters': self.ret_parameters}



class FriServer:
    def __init__(self, hostname, port,  operator_obj, workers_count=2, server_name='fri-node'):
        self.hostname = hostname
        self.port = port

        self.queue = Queue()
        self.operator = operator_obj
        self.stopped = True

        self.__workers = []
        for i in xrange(workers_count):
            thread = FriWorker(self.queue, self.operator)
            thread.setName('%s-FriWorkerThread#%i'%(server_name,i))
            self.__workers.append( thread )

        self.__conn_handler_thread = FriConnectionHandler(hostname, port, self.queue)
        self.__conn_handler_thread.setName('%s-FriConnectionHandler'%(server_name,))

    def start(self):
        self.stopped = False
        for worker in self.__workers:
            worker.start()

        self.__conn_handler_thread.start()
        while self.__conn_handler_thread.status == S_PENDING:
            time.sleep(.1)

        if self.__conn_handler_thread.status == S_ERROR:
            self.stop()
            logger.error('FriServer does not started!')
            return False
        else:
            logger.info('FriServer started!')
            return True

    def stop(self):
        if self.stopped:
            return

        for thread in self.__workers:
            self.queue.put(STOP_THREAD_EVENT)

        self.__conn_handler_thread.stop()

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)

            if self.hostname == '0.0.0.0':
                hostname = '127.0.0.1'
            else:
                hostname = self.hostname

            s.connect((hostname, self.port))
            s.close()
        except socket.error:
            if s:
                s.close()

        #waiting threads finishing... 
        self.queue.join()
        self.stopped = True


class FriConnectionHandler(threading.Thread):
    def __init__(self, host, port, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.hostname = host
        self.port = port
        self.stopped = True
        self.status = S_PENDING

    def __bind_socket(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            #self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.bind((self.hostname, self.port))
            self.sock.listen(2)
        except Exception, err:
            self.status = S_ERROR
            logger.error('[__bind_socket] %s'%err)
        else:
            self.status = S_INWORK
            self.stopped = False


    def run(self):
        logger.info('Starting connection handler thread...')
        self.__bind_socket()
        logger.info('Connection handler thread started!')

        while not self.stopped:
            try:
                (sock, addr) = self.sock.accept()

                if self.stopped:
                    sock.close()
                    break

                self.queue.put(sock)
            except Exception, err:
                logger.error(err)

        self.sock.close()
        logger.info('Connection handler thread stopped!')

    def stop(self):
        self.stopped = True



class FriWorker(threading.Thread):
    def __init__(self, queue, operator):
        threading.Thread.__init__(self)

        self.queue = queue
        self.operator = operator

    def handle_connection(self, sock):
        ret_code = RC_OK
        ret_message = ''
        data = ''

        try:
            while True:
                received = sock.recv(BUF_SIZE)
                if not received:
                    break

                data += received

                if len(received) < BUF_SIZE:
                    break

            if not data:
                raise FriException('empty data block')
        except Exception, err:
            ret_message = '[handle_connection] %s' % err
            ret_code = RC_ERROR
            logger.error(ret_message)

        try:
            if sock:
                sock.send(json.dumps({'ret_code':ret_code, 'ret_message':ret_message}))
                sock.close()
        except Exception, err:
            logger.error('Sending result error: %s' %  err)

        return data

    def run(self):
        logger.info('%s started!'%self.getName())
        while True:
            ret_code = RC_OK
            ret_message = ''

            try:
                sock = self.queue.get()

                if sock == STOP_THREAD_EVENT:
                    logger.info('%s stopped!'%self.getName())
                    break

                data = self.handle_connection(sock)

                packet = self.parse_message(data)
                if not packet.has_key('ret_code'):
                    self.operator.process(FabnetPacketRequest(**packet))
                else:
                    self.operator.callback(FabnetPacketResponse(**packet))
            except Exception, err:
                ret_message = '%s error: %s' % (self.getName(), err)
                logger.error(ret_message)
            finally:
                self.queue.task_done()


    def parse_message(self, data):
        raw_message = json.loads(data)

        if not raw_message.has_key('message_id'):
            raise FriException('[FriWorker.parse_message] message_id does not found in FRI message {%s}'%str(raw_message))

        if (not raw_message.has_key('method')) and (not raw_message.has_key('ret_code')):
            raise FriException('[FriWorker.parse_message] invalid FRI message {%s}'%str(raw_message))

        if raw_message.has_key('method') and (not raw_message.has_key('sender')):
            raise FriException('[FriWorker.parse_message] sender does not found in FRI message {%s}'%str(raw_message))

        return raw_message


class FriClient:
    """class for calling asynchronous operation over FRI protocol"""

    def call(self, node_address, packet, timeout=3.0):
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
                data = json.dumps(packet)

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((hostname, port))

            sock.settimeout(None)

            sock.send(data)

            resp = sock.recv(BUF_SIZE)

            json_object = json.loads(resp)

            return json_object.get('ret_code', RC_UNEXPECTED), json_object.get('ret_message', '')
        except Exception, err:
            return RC_ERROR, '[FriClient] %s'%err
        finally:
            if sock:
                sock.close()


