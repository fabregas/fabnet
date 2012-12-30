#!/usr/bin/python
import time
from datetime import datetime, timedelta
import json
import os
import sys
import random

from multiprocessing import Process, Queue, Event
from Queue import Empty
from fabnet.utils.logger import logger
from nimbus_client.core.fabnet_gateway import FabnetGateway

from fabnet.core.fri_server import FabnetPacketRequest
from fabnet.core.fri_client import FriClient
#logger.setLevel(logging.DEBUG)

from Crypto import Random
Random.atfork()

class SecurityManagerMock:
    def get_user_id(self):
        return 'this is test USER ID string'

    def get_client_cert(self):
        return 'fake cert'

    def get_client_cert_key(self):
        return

    def encrypt(self, data):
        return data

    def decrypt(self, data):
        return data

def to_dt(dt_str):
    parts = dt_str.split(':')
    secs = 0
    if len(parts) > 1:
        secs = int(parts[-2])*60

    return timedelta(0, secs+float(parts[-1]))



def put_data(nodes_list, keys_queue, errors_queue, data_block_size, count=1, ret_queue=None):
    Random.atfork()
    data_block = Random.new().read(data_block_size)
    dt = timedelta()
    for i in xrange(count):
        try:
            node_addr = random.choice(nodes_list)
            fabnet_gw = FabnetGateway(node_addr, SecurityManagerMock())
            t0 = datetime.now()

            primary_key, source_checksum = fabnet_gw.put(data_block, replica_count=2, wait_writes_count=3)
            dt += (datetime.now() - t0)

            keys_queue.put(primary_key)
        except Exception, err:
            errors_queue.put('[ERROR] PUT data block failed! %s'%err)

    if ret_queue:
        ret_queue.put(dt)
    return dt


def get_data(nodes_list, keys_queue, errors_queue, ret_queue=None):
    dt = timedelta()
    while True:
        try:
            primary_key = keys_queue.get(False)
            node_addr = random.choice(nodes_list)
            fabnet_gw = FabnetGateway(node_addr, SecurityManagerMock())
            t0 = datetime.now()
            data = fabnet_gw.get(primary_key, replica_count=2)
            if not data:
                raise Exception('no data found')
            dt += (datetime.now() - t0)
        except Empty:
            break
        except Exception, err:
            errors_queue.put('[ERROR] GET data block failed! %s'%err)
    if ret_queue:
        ret_queue.put(dt)
    return dt


def collect_topology(node_ip):
    print 'Collecting topology on %s'%node_ip
    fri_client = FriClient()
    packet_obj = FabnetPacketRequest(method='TopologyCognition')
    ret_code, ret_msg = fri_client.call(node_ip, packet_obj)

    if ret_code != 0:
        print '[ERROR] TopologyCognition operation does not started: %s'%ret_msg

    time.sleep(3)

def collect_nodes_stat(nodes_list, reset=False):
    collected_stat = {}
    fri_client = FriClient()
    for node_ip in nodes_list:
        #print 'Collecting statistic from %s'%node_ip
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True, parameters={'reset_op_stat': reset})
        ret_packet = fri_client.call_sync(node_ip, packet_obj)
        if ret_packet.ret_code != 0:
            raise Exception('[ERROR] no statistic: %s'%ret_packet.ret_message)

        collected_stat[node_ip] = ret_packet.ret_parameters

    return collected_stat


def parallel_put_data(threads_count, nodes_list, keys_queue, errors_queue, data_block_size, count):
    threads = []
    resp_queue = Queue()
    for i in xrange(threads_count):
        p_count = count/threads_count
        if i == threads_count-1:
            p_count += count % threads_count
        p = Process(target=put_data, args=(nodes_list, keys_queue, errors_queue, data_block_size, p_count, resp_queue))
        threads.append(p)

    time.sleep(1)
    t0 = datetime.now()
    for t in threads:
        t.start()

    for t in threads:
        t.join()
    dt = datetime.now() - t0

    dt_list = []
    while True:
        try:
            c_dt = resp_queue.get(False)
            dt_list.append(c_dt)
        except Empty:
            break

    return dt, sum(dt_list, timedelta())/len(dt_list)


def parallel_get_data(threads_count, nodes_list, keys_queue, errors_queue):
    threads = []
    resp_queue = Queue()
    for i in xrange(threads_count):
        p_count = keys_queue.qsize() / threads_count
        if i == threads_count-1:
            p_count += keys_queue.qsize() % threads_count
        queue = Queue()
        for j in xrange(p_count):
            queue.put(keys_queue.get())

        p = Process(target=get_data, args=(nodes_list, queue, errors_queue, resp_queue))
        threads.append(p)

    time.sleep(1)
    t0 = datetime.now()
    for t in threads:
        t.start()

    for t in threads:
        t.join()
    dt = datetime.now() - t0

    dt_list = []
    while True:
        try:
            c_dt = resp_queue.get(False)
            dt_list.append(c_dt)
        except Empty:
            break

    return dt, sum(dt_list, timedelta())/len(dt_list)


class MemoryMonitor:
    def __init__(self):
        self.proc = None
        self.stop_flag = Event()
        self.ret_queue = Queue()

    def monitor_memory(self, nodes_list, stop_flag, ret_queue):
        min_memory = 10000000
        max_memory = 0
        while not stop_flag.is_set():
            time.sleep(1)
            try:
                stat = collect_nodes_stat(nodes_list)
            except Exception, err:
                print 'monitor_memory error: %s'%err
                continue

            for node_stat in stat.values():
                mem = int(node_stat['memory'])
                if mem < min_memory:
                    min_memory = mem
                if mem > max_memory:
                    max_memory = mem

        ret_queue.put((min_memory, max_memory))

    def start(self, nodes_list):
        self.proc = Process(target=self.monitor_memory, args=(nodes_list, self.stop_flag, self.ret_queue))
        self.proc.start()

    def stop(self):
        if self.proc:
            self.stop_flag.set()
            self.proc.join()
            return self.ret_queue.get()


