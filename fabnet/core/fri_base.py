#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.fri_base
@author Konstantin Andrusenko
@date August 20, 2012

This module contains the implementation of FriServer and FriClient classes.
"""
import ssl
import uuid
import socket
import threading
import time
from Queue import Queue

import json

from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, RC_UNEXPECTED, STOP_THREAD_EVENT, \
                        S_ERROR, S_PENDING, S_INWORK, \
                        BUF_SIZE, CHECK_NEIGHBOURS_TIMEOUT

class FriException(Exception):
    pass


class FabnetPacketRequest:
    def __init__(self, **packet):
        self.message_id = packet.get('message_id', None)
        self.sync = packet.get('sync', False)
        if not self.message_id:
            self.message_id = str(uuid.uuid1())
        self.method = packet.get('method', None)
        self.sender = packet.get('sender', None)
        self.parameters = packet.get('parameters', {})

        self.validate()

    def copy(self):
        return FabnetPacketRequest(**self.to_dict())

    def validate(self):
        if self.message_id is None:
            raise FriException('Invalid packet: message_id does not exists')

        #if self.sender is None:
        #    raise FriException('Invalid packet: sender does not exists')

        if self.method is None:
            raise FriException('Invalid packet: method does not exists')



    def to_dict(self):
        return {'message_id': self.message_id, \
                'method': self.method, \
                'sender': self.sender, \
                'parameters': self.parameters, \
                'sync': self.sync}

    def __str__(self):
        return str(self.__repr__())

    def __repr__(self):
        return '{%s}[%s] %s'%(self.message_id, self.sender, self.method)


class FabnetPacketResponse:
    def __init__(self, **packet):
        self.message_id = packet.get('message_id', None)
        self.ret_code = packet.get('ret_code', RC_OK)
        self.ret_message = packet.get('ret_message', '')
        self.ret_parameters = packet.get('ret_parameters', {})
        self.from_node = packet.get('from_node', None)

    def to_dict(self):
        return {'message_id': self.message_id,
                'ret_code': self.ret_code,
                'ret_message': self.ret_message,
                'ret_parameters': self.ret_parameters,
                'from_node': self.from_node}



class FriServer:
    def __init__(self, hostname, port,  operator_obj, max_workers_count=20, server_name='fri-node',
                    certfile=None, keyfile=None):
        self.hostname = hostname
        self.port = port
        self.certfile = certfile

        self.queue = Queue()
        self.operator = operator_obj
        self.operator.set_node_name(server_name)
        self.operator.set_server(self)
        self.stopped = True

        self.__workers_manager_thread = FriWorkersManager(self.queue, self.operator, \
                    max_count=max_workers_count, workers_name=server_name)
        self.__workers_manager_thread.setName('%s-FriWorkersManager'%(server_name,))

        self.__conn_handler_thread = FriConnectionHandler(hostname, port, self.queue, certfile, keyfile)
        self.__conn_handler_thread.setName('%s-FriConnectionHandler'%(server_name,))

        self.__check_neighbours_thread = CheckNeighboursThread(self.operator)
        self.__check_neighbours_thread.setName('%s-CheckNeighbours'%(server_name,))

    def workers_count(self):
        return self.__workers_manager_thread.get_workers_count()

    def start(self):
        self.stopped = False

        self.__workers_manager_thread.start()
        self.__conn_handler_thread.start()
        self.__check_neighbours_thread.start()

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

        self.__conn_handler_thread.stop()
        self.__check_neighbours_thread.stop()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.certfile:
                sock = ssl.wrap_socket(sock, ca_certs=self.certfile,
                                    cert_reqs=ssl.CERT_REQUIRED)
            sock.settimeout(1.0)

            if self.hostname == '0.0.0.0':
                hostname = '127.0.0.1'
            else:
                hostname = self.hostname

            sock.connect((hostname, self.port))
            sock.close()
        except socket.error:
            if sock:
                sock.close()

        #waiting threads finishing... 
        self.__workers_manager_thread.stop()
        self.__workers_manager_thread.join()
        self.stopped = True


class FriConnectionHandler(threading.Thread):
    def __init__(self, host, port, queue, certfile, keyfile):
        threading.Thread.__init__(self)
        self.queue = queue
        self.hostname = host
        self.port = port
        self.stopped = True
        self.status = S_PENDING
        self.sock = None
        self.certfile = certfile
        self.keyfile = keyfile

    def __bind_socket(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.bind((self.hostname, self.port))
            self.sock.listen(5)
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

                if self.certfile:
                    try:
                        sock = ssl.wrap_socket(sock, server_side=True,
                                    certfile=self.certfile,
                                    keyfile=self.keyfile)
                    except Exception, err:
                        sock.close()
                        raise err

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


class FriWorkersManager(threading.Thread):
    def __init__(self, queue, operator, min_count=2, max_count=20, workers_name='unnamed'):
        threading.Thread.__init__(self)
        self.queue = queue
        self.operator = operator
        self.min_count = min_count
        self.max_count = max_count
        self.workers_name = workers_name

        self.__threads = []
        self.__lock = threading.Lock()
        self.stopped = True

    def run(self):
        self.stopped = False
        self.__lock.acquire()
        try:
            for i in range(self.min_count):
                thread = FriWorker(self.queue, self.operator)
                thread.setName('%s-FriWorkerThread#%i' % (self.workers_name, i))
                self.__threads.append(thread)

            for thread in self.__threads:
                thread.start()
        finally:
            self.__lock.release()

        logger.info('Started work threads (min_count)!')
        not_empty_queue_count = 0
        empty_queue_count = 0

        while not self.stopped:
            try:
                time.sleep(.2)
                if self.queue.qsize() > 0:
                    not_empty_queue_count += 1
                    empty_queue_count = 0
                else:
                    not_empty_queue_count = 0
                    empty_queue_count += 1

                if not_empty_queue_count == 5:
                    self.__spawn_work_threads()
                elif empty_queue_count == 5:
                    self.__stop_work_thread()
                    pass
            except Exception, err:
                ret_message = '%s error: %s' % (self.getName(), err)
                logger.error(ret_message)

        logger.info('Workers manager is stopped!')

    def __spawn_work_threads(self):
        logger.debug('starting new work thread')
        self.__lock.acquire()
        try:
            if len(self.__threads) == self.max_count:
                logger.warning('Need more work threads! But max value is %s'%self.max_count)
                return

            thread = FriWorker(self.queue, self.operator)
            thread.setName('%s-FriWorkerThread#%i'%(self.workers_name, len(self.__threads)))
            thread.start()
            self.__threads.append(thread)
        finally:
            self.__lock.release()

    def __stop_work_thread(self):
        self.__lock.acquire()
        try:
            for_delete = []
            for i, thread in enumerate(self.__threads):
                if not thread.is_alive():
                    for_delete.append(i)

                for i in for_delete:
                    del self.__threads[i]

            if len(self.__threads) == self.min_count:
                logger.debug('trying stopping worker but min threads count occured')
                return

            self.queue.put(STOP_THREAD_EVENT)
        finally:
            self.__lock.release()

        logger.debug('stopped one work thread')


    def stop(self):
        self.stopped = True
        self.__lock.acquire()
        try:
            act_count = 0
            for thread in self.__threads:
                if thread.is_alive():
                    act_count += 1

            for i in xrange(act_count):
                self.queue.put(STOP_THREAD_EVENT)

            self.queue.join()

            for thread in self.__threads:
                if thread.is_alive():
                    thread.join()
        finally:
            self.__lock.release()

    def get_workers_count(self):
        self.__lock.acquire()
        try:
            return len(self.__threads)
        finally:
            self.__lock.release()


class FriWorker(threading.Thread):
    def __init__(self, queue, operator):
        threading.Thread.__init__(self)

        self.queue = queue
        self.operator = operator

    def handle_connection(self, sock):
        data = ''

        while True:
            received = sock.recv(BUF_SIZE)
            if not received:
                break

            data += received

            if len(received) < BUF_SIZE:
                break

        if not data:
            raise FriException('empty data block')

        return data

    def run(self):
        logger.info('%s started!'%self.getName())
        ok_msg = json.dumps({'ret_code': RC_OK, 'ret_message': 'ok'})

        while True:
            ret_code = RC_OK
            ret_message = ''
            data = ''

            try:
                sock = self.queue.get()

                if sock == STOP_THREAD_EVENT:
                    logger.info('%s stopped!'%self.getName())
                    break

                data = self.handle_connection(sock)

                packet = self.parse_message(data)
                is_sync = packet.get('sync', False)
                if not is_sync:
                    sock.sendall(ok_msg)
                    sock.shutdown(socket.SHUT_WR)
                    sock.close()
                    sock = None

                if not packet.has_key('ret_code'):
                    pack = FabnetPacketRequest(**packet)

                    ret_packet = self.operator.process(pack)

                    if not is_sync:
                        if ret_packet:
                            self.operator.send_to_sender(pack.sender, ret_packet)
                    else:
                        if not ret_packet:
                            ret_packet = FabnetPacketResponse()
                        sock.sendall(json.dumps(ret_packet.to_dict()))
                        sock.shutdown(socket.SHUT_WR)
                        sock.close()
                else:
                    self.operator.callback(FabnetPacketResponse(**packet))
            except Exception, err:
                ret_message = '%s error: %s' % (self.getName(), err)
                logger.error(ret_message)
                try:
                    if sock:
                        sock.sendall(json.dumps({'ret_code': RC_ERROR, 'ret_message': ret_message}))
                        sock.shutdown(socket.SHUT_WR)
                        sock.close()
                except Exception, err:
                    logger.error("Can't send error message to socket: %s"%err)
                    self._close_socket(sock)
            finally:
                self.queue.task_done()

    def _close_socket(self, sock):
        try:
            sock.close()
        except Exception, err:
            logger.error('Closing client socket error: %s'%err)

    def parse_message(self, data):
        raw_message = json.loads(data)

        if not raw_message.has_key('message_id'):
            raise FriException('[FriWorker.parse_message] message_id does not found in FRI message {%s}'%str(raw_message))

        if (not raw_message.has_key('method')) and (not raw_message.has_key('ret_code')):
            raise FriException('[FriWorker.parse_message] invalid FRI message {%s}'%str(raw_message))

        if raw_message.has_key('method') and (not raw_message.has_key('sender')):
            raise FriException('[FriWorker.parse_message] sender does not found in FRI message {%s}'%str(raw_message))

        return raw_message


class CheckNeighboursThread(threading.Thread):
    def __init__(self, operator):
        threading.Thread.__init__(self)
        self.operator = operator
        self.stopped = True

    def run(self):
        self.stopped = False
        logger.info('Check neighbours thread is started!')

        while not self.stopped:
            try:
                self.operator.check_neighbours()

                for i in range(CHECK_NEIGHBOURS_TIMEOUT):
                    if self.stopped:
                        break
                    time.sleep(1)
            except Exception, err:
                logger.error('[CheckNeighboursThread] %s'%err)

        logger.info('Check neighbours thread is stopped!')

    def stop(self):
        self.stopped = True



#------------- FRI client class ----------------------------------------------

class FriClient:
    """class for calling asynchronous operation over FRI protocol"""
    def __init__(self, certfile=None):
        self.certfile = certfile

    def __int_call(self, node_address, packet, timeout=3.0):
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
            if self.certfile:
                sock = ssl.wrap_socket(sock, ca_certs=self.certfile,
                                    cert_reqs=ssl.CERT_REQUIRED)
            sock.settimeout(timeout)
            sock.connect((hostname, port))

            sock.settimeout(None)

            sock.sendall(data)

            data = ''
            while True:
                received = sock.recv(BUF_SIZE)
                if not received:
                    break

                data += received

                if len(received) < BUF_SIZE:
                    break

            return data
        finally:
            if sock:
                sock.close()

    def call(self, node_address, packet, timeout=3.0):
        try:
            data = self.__int_call(node_address, packet, timeout)

            json_object = json.loads(data)

            return json_object.get('ret_code', RC_UNEXPECTED), json_object.get('ret_message', '')
        except Exception, err:
            return RC_ERROR, '[FriClient] %s' % err


    def call_sync(self, node_address, packet, timeout=3.0):
        try:
            data = self.__int_call(node_address, packet, timeout)

            json_object = json.loads(data)

            return FabnetPacketResponse(**json_object)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='[FriClient] %s'%err)


