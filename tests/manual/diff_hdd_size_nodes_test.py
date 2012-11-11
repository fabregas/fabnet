import sys
import time
import os
import logging
import threading
import json
import random
import subprocess
import signal
import sqlite3
import shutil
import hashlib
import string
from datetime import datetime
from fabnet.utils.logger import logger

from fabnet.core.constants import ET_INFO
from fabnet.core.fri_server import FriClient, FabnetPacketRequest
from fabnet.utils.db_conn import DBConnection
from fabnet.monitor.monitor_operator import MONITOR_DB

from Crypto import Random

GB_1 = 1024*1024
GB_2 = 2*GB_1
GB_1_5 = int(1.5*GB_1)


monitoring_home = '/tmp/monitor_node_home'

def make_fake_hdd(name, size, dev='/dev/loop0'):
    os.system('dd if=/dev/zero of=/tmp/%s bs=1024 count=%s'%(name, size))
    os.system('sudo umount /tmp/mnt_%s'%name)
    os.system('sudo losetup -d %s'%dev)
    os.system('sudo losetup %s /tmp/%s'%(dev, name))
    os.system('sudo mkfs -t ext2 -m 1 -v %s'%dev)
    os.system('sudo mkdir /tmp/mnt_%s'%name)
    os.system('sudo mount -t ext2 %s /tmp/mnt_%s'%(dev, name))
    os.system('sudo chmod 777 /tmp/mnt_%s -R'%name)

    return '/tmp/mnt_%s'%name

def destroy_fake_hdd(name, dev='/dev/loop0'):
    os.system('sudo umount /tmp/mnt_%s'%name)
    os.system('sudo losetup -d %s'%dev)
    os.system('sudo rm /tmp/%s'%name)


def create_network(ip_addr):
    addresses = []
    processes = []

    start_port = 1990
    for i in xrange(len(hdds_size)):
        node_name = 'node%02i'%i
        destroy_fake_hdd(node_name, '/dev/loop%i'%i)
        homedir = make_fake_hdd(node_name, hdds_size[i], '/dev/loop%i'%i)

        if not addresses:
            n_node = 'init-fabnet'
        else:
            n_node = random.choice(addresses)

        address = '%s:%s'%(ip_addr, start_port)
        addresses.append(address)
        start_port += 1

        logger.warning('{SNP} STARTING NODE %s'%address)
        p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, node_name, homedir, 'DHT'])
        processes.append(p)
        logger.warning('{SNP} PROCESS STARTED')


    os.system('rm -rf %s'%monitoring_home)
    os.system('mkdir %s'%monitoring_home)
    address = '%s:%s'%(ip_addr, 1989)
    logger.warning('{SNP} STARTING MONITORING NODE %s'%address)
    p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, 'monitor', monitoring_home, 'Monitor'])
    processes.append(p)
    logger.warning('{SNP} PROCESS STARTED')

    print 'Network is started!'
    return addresses, processes

def destroy_network(processes):
    for proc in processes:
        proc.send_signal(signal.SIGINT)

    for proc in processes:
        proc.wait()

    print 'Network is destroyed!'
    for i in xrange(len(processes)):
        destroy_fake_hdd('node%02i'%i, '/dev/loop%i'%i)
    print 'Virtual HDD devices are destroyed!'


def test_network(addresses, stat_buffer, perc=10):
    client = FriClient()
    rs = 2**160
    print '-'*100
    for address in addresses:
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        ret_packet = client.call_sync(address, packet_obj)
        dht_info = ret_packet.ret_parameters['dht_info']
        start = dht_info['range_start']
        end = dht_info['range_end']
        len_r = long(end, 16) - long(start, 16)
        stat_buffer.write('%s, %s, %s, %s\n'%(address, len_r, len_r*100./rs, dht_info['free_size_percents']))
        print 'On node %s: {%s-%s}[%x]'%(address, start, end, len_r)
        print '           %s KB / %s KB (%s perc. free)'% (dht_info['range_size']/1024, \
                        dht_info['replicas_size']/1024, dht_info['free_size_percents'])
    stat_buffer.write('\n')
    print '-'*100


    print 'PUT DATA...'
    while True:
        for i in xrange(50):
            data = Random.new().read(1024*1024)
            checksum =  hashlib.sha1(data).hexdigest()

            params = {'checksum': checksum, 'wait_writes_count': 2}
            packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=data, sync=True)

            ret_packet = client.call_sync(random.choice(addresses), packet_obj)
            if ret_packet.ret_code != 0:
                print 'ERROR! Cant put data block to network! Details: %s'%ret_packet.ret_message


        full_cnt = 0
        dht_info_lst = []
        max_perc = 0
        for address in addresses:
            packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
            for i in xrange(3):
                ret_packet = client.call_sync(address, packet_obj)
                if ret_packet.ret_code == 0:
                    break
            else:
                print 'ERROR! NodeStatistic failed on %s with message: %s'%(address, ret_packet.ret_message)
                return

            dht_info = ret_packet.ret_parameters['dht_info']

            if max_perc < dht_info['free_size_percents']:
                max_perc = dht_info['free_size_percents']

            if dht_info['free_size_percents'] < perc:
                full_cnt += 1

            dht_info_lst.append((address, dht_info))

        if full_cnt:
            print '='*100
            for address, dht_info in dht_info_lst:
                start = dht_info['range_start']
                end = dht_info['range_end']
                len_r = long(end, 16) - long(start, 16)
                stat_buffer.write('%s, %s, %s, %s\n'%(address, len_r, len_r*100./rs, dht_info['free_size_percents']))

                print 'On node %s: {%s-%s}[%x]'%(address, start, end, len_r)
                print '           %s KB / %s KB (%s perc. free)'% (dht_info['range_size']/1024, \
                                dht_info['replicas_size']/1024, dht_info['free_size_percents'])
            stat_buffer.write('\n')
            print '='*100

            if max_perc > 10:
                return False
            return True


def destroy_data(address, nodenum):
    homedir = '/tmp/mnt_node%02i'%nodenum
    packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
    client = FriClient()
    ret_packet = client.call_sync(address, packet_obj)
    dht_info = ret_packet.ret_parameters['dht_info']
    size = dht_info['range_size'] + dht_info['replicas_size']

    print '='*100
    ret = os.system('sudo rm -rf %s/dht_range/%s_%s/*'%(homedir, dht_info['range_start'], dht_info['range_end']))
    if ret:
        raise Exception('Node %s range data does not destroyed!'%address)
    print '='*100
    ret = os.system('sudo rm -rf %s/dht_range/replica_data/*'%homedir)
    if ret:
        raise Exception('Node %s replica data does not destroyed!'%address)

    return size


def call_repair_data(address, out_streem):
    dbpath = os.path.join(monitoring_home, MONITOR_DB)
    dbconn = DBConnection(dbpath)
    dbconn.execute("DELETE FROM notification")

    t0 = datetime.now()

    client = FriClient()
    packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters={})
    rcode, rmsg = client.call(address, packet_obj)
    if rcode != 0:
        raise Exception('RepairDataBlocks does not started. Details: %s'%rmsg)

    cnt = 0
    while cnt != len(hdds_size):
        cnt = dbconn.select_one("SELECT count(*) FROM notification WHERE notify_type='%s'"%ET_INFO)
        time.sleep(.2)

    dt = datetime.now() - t0

    events = dbconn.select("SELECT node_address, notify_type, notify_msg, notify_dt FROM notification")
    for event in events:
        if event[1] != ET_INFO:
            raise Exception('RepairDataBlocks are failed at %s: %s'%(event[0], event[2]))
        out_streem.write('[%s][%s][%s] %s\n'%(event[0], event[3], event[1], event[2]))

        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        client = FriClient()
        ret_packet = client.call_sync(event[0], packet_obj)
        out_streem.write('[%s] stat: %s\n'%(event[0], ret_packet.parameters['methods_stat']))

    return dt



if __name__ == '__main__':
    is_pull_test = False
    if is_pull_test:
        hdds_size = [GB_1_5, GB_1, GB_1, GB_2, GB_2, GB_1, GB_1_5, GB_2]
    else:
        hdds_size = [GB_2, GB_2, GB_2, GB_2, GB_2, GB_2, GB_2, GB_2]

    addresses, processes = create_network('127.0.0.1')
    try:
        time.sleep(3)
        stat_f_obj = open('/tmp/pull_subranges_test_results.csv', 'w')
        stat_f_obj.write('#Address, RangeLen, RangePart, FreeSpace\n')

        if is_pull_test:
            while True:
                is_all_full = test_network(addresses, stat_f_obj)
                if is_all_full:
                    break
                time.sleep(40)
        else:
            test_network(addresses, stat_f_obj, 25)
            stat_f_obj.write('\n\n#process RepairDataBlocks operation over ALL valid data...\n')
            proc_time = call_repair_data(addresses[0], stat_f_obj)
            stat_f_obj.write('Process time: %s\n'%proc_time)

            destroyed = destroy_data(addresses[1], 1)
            stat_f_obj.write('\n\n#process RepairDataBlocks operation after %.2f GB data lost from one node...\n'%(destroyed/1024./1024/1024))
            proc_time = call_repair_data(addresses[0], stat_f_obj)
            stat_f_obj.write('Process time: %s\n'%proc_time)

            destroyed = destroy_data(addresses[1], 1)
            destroyed += destroy_data(addresses[3], 3)
            stat_f_obj.write('\n\n#process RepairDataBlocks operation after %.2f GB data lost from one node...\n'%(destroyed/1024./1024/1024))
            proc_time = call_repair_data(addresses[0], stat_f_obj)
            stat_f_obj.write('Process time: %s\n'%proc_time)
    finally:
        destroy_network(processes)
        stat_f_obj.close()
        print 'statistic saved to /tmp/pull_subranges_test_results.csv'

