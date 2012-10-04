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
import struct
import zlib
from datetime import datetime
from Queue import Queue

import json

from fabnet.utils.logger import logger
from fabnet.core.constants import RC_OK, RC_ERROR, RC_UNEXPECTED, STOP_THREAD_EVENT, \
                        S_ERROR, S_PENDING, S_INWORK, \
                        BUF_SIZE, CHECK_NEIGHBOURS_TIMEOUT, FRI_CLIENT_TIMEOUT, \
                        MIN_WORKERS_COUNT, MAX_WORKERS_COUNT, \
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
        cur_thread = threading.current_thread()
        cur_thread.setName('%s-MAIN'%server_name)

    def workers_stat(self):
        return self.__workers_manager_thread.get_workers_stat()

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

        self.operator.stop()
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

        self.__workers_manager_thread.stop()

        #waiting threads finishing... 
        self.__workers_manager_thread.join()
        self.__conn_handler_thread.join()
        self.__check_neighbours_thread.join()
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
    def __init__(self, queue, operator, min_count=MIN_WORKERS_COUNT, \
                    max_count=MAX_WORKERS_COUNT, workers_name='unnamed'):
        threading.Thread.__init__(self)
        self.queue = queue
        self.operator = operator
        self.min_count = min_count
        self.max_count = max_count
        self.workers_name = workers_name

        self.__threads = []
        self.__threads_idx = 0
        self.__lock = threading.Lock()
        self.stopped = True

    def get_workers_stat(self):
        act_count = busy_count = 0
        self.__lock.acquire()
        try:

            for thread in self.__threads:
                if thread.is_alive():
                    act_count += 1
                    is_busy = thread.is_busy()
                    if is_busy:
                        busy_count += 1
        finally:
            self.__lock.release()

        return act_count, busy_count

    def run(self):
        self.stopped = False
        self.__lock.acquire()
        try:
            for i in range(self.min_count):
                thread = FriWorker(self.queue, self.operator)
                thread.setName('%s-FriWorkerThread#%i' % (self.workers_name, i))
                self.__threads.append(thread)
                self.__threads_idx = self.min_count

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

                act, busy = self.get_workers_stat()
                if not_empty_queue_count >= 5:
                    if act == busy:
                        self.__spawn_work_threads()
                elif empty_queue_count >= 15:
                    if (act - busy) > self.min_count:
                        self.__stop_work_thread()
            except Exception, err:
                ret_message = '%s error: %s' % (self.getName(), err)
                logger.error(ret_message)

        logger.info('Workers manager is stopped!')

    def __spawn_work_threads(self):
        logger.debug('starting new work thread')
        self.__lock.acquire()
        if self.stopped:
            return

        try:
            if len(self.__threads) == self.max_count:
                return

            thread = FriWorker(self.queue, self.operator)
            thread.setName('%s-FriWorkerThread#%i'%(self.workers_name, self.__threads_idx))
            self.__threads_idx += 1
            thread.start()
            self.__threads.append(thread)
        finally:
            self.__lock.release()

    def __stop_work_thread(self):
        self.__lock.acquire()
        if self.stopped:
            return

        try:
            for_delete = []
            for i, thread in enumerate(self.__threads):
                if not thread.is_alive():
                    logger.debug('Worker %s is not alive! delete it...'%thread.getName())
                    for_delete.append(thread)

            for thr in for_delete:
                del self.__threads[self.__threads.index(thr)]

            if len(self.__threads) <= self.min_count:
                logger.debug('trying stopping worker but min threads count occured')
                return

            self.queue.put(STOP_THREAD_EVENT)
        finally:
            self.__lock.release()

        logger.debug('stopped one work thread')


    def stop(self):
        self.__lock.acquire()
        self.stopped = True
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
        except Exception, err:
            logger.error('stopping error: %s'%err)
        finally:
            self.__lock.release()



class FriWorker(threading.Thread):
    def __init__(self, queue, operator):
        threading.Thread.__init__(self)

        self.queue = queue
        self.operator = operator
        self.__busy_flag = threading.Event()

    def is_busy(self):
        return self.__busy_flag.is_set()

    def handle_connection(self, sock):
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


    def run(self):
        logger.info('%s started!'%self.getName())
        ok_packet = FabnetPacketResponse(ret_code=RC_OK, ret_message='ok')
        ok_msg = ok_packet.dump()

        while True:
            ret_code = RC_OK
            ret_message = ''
            data = ''
            sock = None

            try:
                self.__busy_flag.clear()
                sock = self.queue.get()

                if sock == STOP_THREAD_EVENT:
                    logger.info('%s stopped!'%self.getName())
                    break

                self.__busy_flag.set()

                packet = self.handle_connection(sock)

                is_sync = packet.get('sync', False)
                if not is_sync:
                    sock.sendall(ok_msg)
                    sock.shutdown(socket.SHUT_WR)
                    sock.close()
                    sock = None

                if not packet.has_key('ret_code'):
                    pack = FabnetPacketRequest(**packet)

                    ret_packet = self.operator.process(pack)

                    try:
                        if not is_sync:
                            if ret_packet:
                                self.operator.send_to_sender(pack.sender, ret_packet)
                        else:
                            if not ret_packet:
                                ret_packet = FabnetPacketResponse()
                            sock.sendall(ret_packet.dump())
                            sock.shutdown(socket.SHUT_WR)
                            sock.close()
                            sock = None
                    finally:
                        self.operator.after_process(pack, ret_packet)
                else:
                    self.operator.callback(FabnetPacketResponse(**packet))
            except Exception, err:
                ret_message = '%s error: %s' % (self.getName(), err)
                logger.error(ret_message)
                try:
                    if sock:
                        err_packet = FabnetPacketResponse(ret_code=RC_ERROR, ret_message=ret_message)
                        sock.sendall(err_packet.dump())
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
                t0 = datetime.now()

                self.operator.check_neighbours()

                proc_dt = datetime.now() - t0
            except Exception, err:
                logger.error('[CheckNeighboursThread] %s'%err)
            finally:
                wait_seconds = CHECK_NEIGHBOURS_TIMEOUT - proc_dt.seconds
                for i in range(wait_seconds):
                    if self.stopped:
                        break
                    time.sleep(1)

        logger.info('Check neighbours thread is stopped!')

    def stop(self):
        self.stopped = True



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


