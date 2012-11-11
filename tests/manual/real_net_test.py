#!/usr/bin/python
import time
from datetime import datetime, timedelta
import json
import os
import sys

from multiprocessing import Process, Queue

from fabnet.utils.logger import logger
from client.fabnet_gateway import FabnetGateway

from fabnet.core.fri_server import FriClient, FabnetPacketRequest
#logger.setLevel(logging.DEBUG)

NODEPORT = 1987

CHUNKS_PER_NODE = 5
DATA_BLOCK_SIZE = 1*1024*1024 #1 MB

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



def PutFileRoutine(node_ip, queue):
    from Crypto import Random
    dt = timedelta()
    for i in xrange(CHUNKS_PER_NODE):
        try:
            data_block = Random.new().read(DATA_BLOCK_SIZE)
            fabnet_gw = FabnetGateway(node_ip, SecurityManagerMock())
            t0 = datetime.now()
            primary_key, source_checksum = fabnet_gw.put(data_block, replica_count=2, wait_writes_count=3)
            dt += (datetime.now() - t0)

            queue.put(primary_key)
        except Exception, err:
            print '[ERROR] PUT data block failed! %s'%err
    print 'Put data block (%s bytes) to %s avg time: %s'% (DATA_BLOCK_SIZE, node_ip, dt/CHUNKS_PER_NODE)


def GetFileRoutine(node_ip, queue):
    dt = timedelta()
    for i in xrange(CHUNKS_PER_NODE):
        try:
            if queue.empty():
                break
            primary_key = queue.get()
            fabnet_gw = FabnetGateway(node_ip, SecurityManagerMock())
            t0 = datetime.now()
            data = fabnet_gw.get(primary_key, replica_count=2)
            dt += (datetime.now() - t0)
            if len(data) != DATA_BLOCK_SIZE:
                raise Exception('Invalid data block size - %s'%len(data))
        except Exception, err:
            print '[ERROR] PUT data block failed! %s'%err
    print 'Get data block (%s bytes) from %s avg time: %s'% (DATA_BLOCK_SIZE, node_ip, dt/CHUNKS_PER_NODE)


def collect_topology(node_ip):
    print 'Collecting topology on %s'%node_ip
    fri_client = FriClient()
    packet_obj = FabnetPacketRequest(method='TopologyCognition')
    ret_code, ret_msg = fri_client.call(node_ip, packet_obj)

    if ret_code != 0:
        print '[ERROR] TopologyCognition operation does not started: %s'%ret_msg

    time.sleep(3)

def collect_nodes_stat(nodes_ip):
    collected_stat = {}
    fri_client = FriClient()
    for node_ip in nodes_ip:
        print 'Collecting statistic from %s'%node_ip
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        ret_packet = fri_client.call_sync(node_ip, packet_obj)
        if ret_packet.ret_code != 0:
            print '[ERROR] no statistic: %s'%ret_packet.ret_message

        collected_stat[node_ip] = ret_packet.ret_parameters

    f_obj = open('/tmp/real_net_test.stat', 'w')
    f_obj.write(json.dumps(collected_stat))
    f_obj.close()
    print 'Statistic saved to /tmp/real_net_test.stat'


def put_file_test(nodes_ip):
    threads = []
    queue = Queue()
    for node_ip in nodes_ip:
        p = Process(target=PutFileRoutine, args=(node_ip, queue))
        threads.append(p)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    return queue

def get_file_test(queue, nodes_ip):
    threads = []
    for node_ip in nodes_ip:
        p = Process(target=GetFileRoutine, args=(node_ip, queue))
        threads.append(p)

    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'usage: %s <file with nodes IPs>'%sys.argv[0]
        sys.exit(1)

    if not os.path.exists(sys.argv[1]):
        print 'file %s does not exists'%sys.argv[1]
        sys.exit(1)

    ips = []
    for line in open(sys.argv[1]).readlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if ':' not in line:
            line  += ':%s'%NODEPORT
        ips.append(line)
    if not ips:
        print 'no IP addresses found!'
        sys.exit(1)

    collect_topology(ips[0])
    queue = put_file_test(ips)
    get_file_test(queue, ips)
    collect_nodes_stat(ips)

    print 'DONE. Please, collect syslog from nodes and take <fabnet_home>/fabnet_topology.db from %s'%ips[0]
