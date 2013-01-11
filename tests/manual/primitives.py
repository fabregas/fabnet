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
from fabnet.core.fri_server import FabnetPacketRequest
from fabnet.core.fri_client import FriClient
from fabnet.core.fri_base import RamBasedBinaryData
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.monitor.monitor_operator import MONITOR_DB
from fabnet.dht_mgmt.hash_ranges_table import HashRangesTable

from Crypto import Random


monitoring_home = '/tmp/monitor_node_home'

def make_fake_hdd(name, size, dev='/dev/loop0'):
    if os.path.exists('/tmp/%s'%name):
        print 'warning: virtual HDD is already exists at /tmp/%s'%name
        return '/tmp/mnt_%s'%name

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
    if not os.path.exists('/tmp/mnt_%s'%name):
        return
    os.system('sudo umount /tmp/mnt_%s'%name)
    os.system('sudo losetup -d %s'%dev)
    os.system('sudo rm -rf /tmp/mnt_%s'%name)
    os.system('sudo rm /tmp/%s'%name)


def create_network(ip_addr, hdds_size):
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
        p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, node_name, homedir, 'DHT', '--nodaemon'])
        processes.append(p)
        wait_node(address)
        logger.warning('{SNP} NODE %s IS STARTED'%address)

    print 'Network is started!'
    return addresses, processes

def reboot_nodes(processes, addresses, timeout=None):
    ret_processes = []
    try:
        for i, address in enumerate(addresses):
            node_name = 'node%02i'%i
            homedir = '/tmp/mnt_%s'%node_name
            proc = processes[i]

            #stop node
            logger.warning('{SNP} STOPPING NODE %s'%address)
            proc.send_signal(signal.SIGINT)
            proc.wait()
            logger.warning('{SNP} NODE %s IS STOPPED'%address)

            if timeout:
                time.sleep(timeout)

            #start node
            while True:
                n_node = random.choice(addresses)
                if n_node != address:
                    break
            logger.warning('{SNP} STARTING NODE %s'%address)
            p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, node_name, homedir, 'DHT', '--nodaemon'])
            ret_processes.append(p)
            wait_node(address)
            logger.warning('{SNP} NODE %s IS STARTED'%address)
    except Exception, err:
        destroy_network(ret_processes)
        raise err

    time.sleep(1)
    return ret_processes


def create_monitor(neigbour):
    os.system('rm -rf %s'%monitoring_home)
    os.system('mkdir %s'%monitoring_home)
    address = '%s:%s'%('127.0.0.1', 1989)
    logger.warning('{SNP} STARTING MONITORING NODE %s'%address)
    mon_p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fabnet-node', address, neigbour, 'monitor', monitoring_home, 'Monitor', '--nodaemon'])
    logger.warning('{SNP} PROCESS STARTED')
    time.sleep(1)
    return mon_p

def destroy_network(processes, destroy_hdds=True):
    for proc in processes:
        proc.send_signal(signal.SIGINT)

    for proc in processes:
        proc.wait()

    print 'Network is destroyed!'
    if destroy_hdds:
        for i in xrange(len(processes)):
            destroy_fake_hdd('node%02i'%i, '/dev/loop%i'%i)
        print 'Virtual HDD devices are destroyed!'


def put_data_to_network(addresses, stat_buffer, perc=10):
    client = FriClient()
    rs = 2**160
    print '-'*100
    for address in addresses:
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        ret_packet = client.call_sync(address, packet_obj)
        if ret_packet.ret_code:
            raise Exception('ERROR! NodeStatistic failed on %s with message: %s'%(address, ret_packet.ret_message))

        dht_info = ret_packet.ret_parameters['DHTInfo']
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
            packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=RamBasedBinaryData(data), sync=True)

            ret_packet = client.call_sync(random.choice(addresses), packet_obj)
            if ret_packet.ret_code != 0:
                print 'ERROR! Cant put data block to network! Details: %s'%ret_packet.ret_message


        max_perc, full_cnt, dht_info_lst = get_maximum_free_space(addresses, perc)
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

def get_maximum_free_space(addresses, perc=0):
    max_perc = 0
    full_cnt = 0
    dht_info_lst = []
    client = FriClient()
    for address in addresses:
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        for i in xrange(3):
            ret_packet = client.call_sync(address, packet_obj)
            if ret_packet.ret_code == 0:
                break
        else:
            raise Exception('ERROR! NodeStatistic failed on %s with message: %s'%(address, ret_packet.ret_message))

        dht_info = ret_packet.ret_parameters['DHTInfo']

        if max_perc < dht_info['free_size_percents']:
            max_perc = dht_info['free_size_percents']

        if dht_info['free_size_percents'] < perc:
            full_cnt += 1

        dht_info_lst.append((address, dht_info))

    return max_perc, full_cnt, dht_info_lst

def destroy_data(address, nodenum):
    homedir = '/tmp/mnt_node%02i'%nodenum
    packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
    client = FriClient()
    ret_packet = client.call_sync(address, packet_obj)
    dht_info = ret_packet.ret_parameters['DHTInfo']
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


def call_repair_data(address, out_streem, expect_res, invalid_node=None):
    dbconn = DBConnection("dbname=%s user=postgres"%MONITOR_DB)
    dbconn.execute("DELETE FROM notification")

    client = FriClient()
    params = {}
    packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
    ret_packet = client.call_sync(address, packet_obj)
    dht_info = ret_packet.ret_parameters['dht_info']
    free_size = dht_info['free_size_percents']
    if invalid_node:
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        ret_packet = client.call_sync(invalid_node, packet_obj)
        dht_info = ret_packet.ret_parameters['DHTInfo']
        start = dht_info['range_start']
        end = dht_info['range_end']
        params = {'check_range_start': start, 'check_range_end': end}


    t0 = datetime.now()

    packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters=params)
    rcode, rmsg = client.call(address, packet_obj)
    if rcode != 0:
        raise Exception('RepairDataBlocks does not started. Details: %s'%rmsg)

    cnt = 0
    try_cnt = 0
    while cnt != expect_res:
        if try_cnt == 100:
            print 'Notifications count: %s, but expected: %s'%(cnt, expect_res)
            try_cnt = 0
        try_cnt += 1
	
        try:
            cnt = dbconn.select_one("SELECT count(*) FROM notification WHERE notify_topic='RepairDataBlocks statistic'")
        except Exception, err:
            print 'DB ERROR: %s'%err
        time.sleep(.2)

    dt = datetime.now() - t0

    events = dbconn.select("SELECT node_address, notify_type, notify_msg, notify_dt FROM notification WHERE notify_topic='RepairDataBlocks statistic'")
    for event in events:
        out_streem.write('[%s][%s][%s] %s\n'%(event[0], event[3], event[1], event[2]))

        #packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        #client = FriClient()
        #ret_packet = client.call_sync(event[0], packet_obj)
        #out_streem.write('[%s] stat: %s\n'%(event[0], ret_packet.ret_parameters['methods_stat']))


    packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
    ret_packet = client.call_sync(address, packet_obj)
    dht_info = ret_packet.ret_parameters['DHTInfo']
    post_free_size = dht_info['free_size_percents']

    if post_free_size < free_size:
        out_streem.write('INVALID RESTORED RANGE FREE SIZE. BEFORE=%s, AFTER=%s\n'%(free_size, post_free_size))

    out_streem.write('Process time: %s\n'%dt)
    dbconn.close()


def create_virt_net(nodes_count, port_move=0):
    addresses = []
    processes = []
    for unuse in range(nodes_count):
        if not addresses:
            i = 1900+port_move
            if port_move:
                n_node = '127.0.0.1:%i'%(i-1)
            else:
                n_node = 'init-fabnet'
        else:
            n_node = random.choice(addresses)
            i = int(addresses[-1].split(':')[-1])+1
            wait_node(n_node)

        address = '127.0.0.1:%s'%i
        addresses.append(address)

        home = '/tmp/node_%s'%i
        if os.path.exists(home):
            shutil.rmtree(home)
        os.mkdir(home)

        args = ['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, '%.02i'%i, home, 'DHT', '--nodaemon']
        #if DEBUG:
        #args.append('--debug')
        p = subprocess.Popen(args)
        time.sleep(0.1)

        processes.append(p)
        if len(addresses) > 2:
            check_stat(address)

    return addresses, processes

def wait_node(node):
    client = FriClient()
    while True:
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        ret_packet = client.call_sync(node, packet_obj)
        if ret_packet.ret_code:
            print 'Node %s does not init FRI server yet. Waiting it...'%node
            time.sleep(.5)
            continue
        if ret_packet.ret_parameters['DHTInfo']['status'] != 'normwork':
            print 'Node %s does not init as DHT member yet. Waiting...'%node
            time.sleep(.5)
            continue
        break

def check_stat(address):
    client = FriClient()

    while True:
        try:
            packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
            ret_packet = client.call_sync(address, packet_obj)
            if ret_packet.ret_code:
                time.sleep(.5)
                continue

            uppers_balance = int(ret_packet.ret_parameters['NeighboursInfo'][u'uppers_balance'])
            superiors_balance = int(ret_packet.ret_parameters['NeighboursInfo'][u'superiors_balance'])
            if uppers_balance >= 0 and superiors_balance >= 0:
                if ret_packet.ret_parameters['DHTInfo']['status'] == 'normwork':
                    break
                print 'Node %s is not initialized as DHT member yet! Waiting...'%(address)
            else:
                print 'Node %s is not balanced yet! Waiting...'%address
            time.sleep(.5)
        except Exception, err:
            logger.error('ERROR: %s'%err)
            raise err


def print_ranges(addresses, out_streem):
    client = FriClient()
    out_streem.write('\nRANGES SIZES:\n')
    ranges = {}
    for address in addresses:
        packet_obj = FabnetPacketRequest(method='NodeStatistic', sync=True)
        ret_packet = client.call_sync(address, packet_obj)
        if ret_packet.ret_code:
            raise Exception('NodeStatistic failed on %s: %s'%(address, ret_packet.ret_message))

        start = ret_packet.ret_parameters['DHTInfo']['range_start']
        end = ret_packet.ret_parameters['DHTInfo']['range_end']
        range_size = ret_packet.ret_parameters['DHTInfo']['range_size']
        replicas_size = ret_packet.ret_parameters['DHTInfo']['replicas_size']
        ranges[address] = (start, end, range_size, replicas_size)

    h_ranges = HashRangesTable()
    for address, (start, end, _, _) in ranges.items():
        h_ranges.append(long(start,16), long(end,16), address)

    for h_range in h_ranges.iter_table():
        start = h_range.start
        end = h_range.end
        address = h_range.node_address
        len_r = end - start
        out_streem.write('On node %s: {%040x-%040x}[%040x] = %s KB (%s KB)\n'%(address, start, end, len_r,\
                 ranges[address][2]/1024, ranges[address][3]/1024))
    out_streem.flush()

    end = -1
    for h_range in h_ranges.iter_table():
        if end+1 != h_range.start:
            raise Exception('Distributed range table is not full!')
        end = h_range.end
    return ranges


def put_data_blocks(addresses, block_size=1024, blocks_count=1000):
    client = FriClient()
    keys = []
    for i in xrange(blocks_count):
        data = Random.new().read(block_size)
        checksum =  hashlib.sha1(data).hexdigest()

        params = {'checksum': checksum, 'wait_writes_count': 2}
        packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=RamBasedBinaryData(data), sync=True)
        nodeaddr = random.choice(addresses)

        ret_packet = client.call_sync(nodeaddr, packet_obj)
        if ret_packet.ret_code != 0:
            raise Exception('ClientPutData failed on %s: %s'%(nodeaddr, ret_packet.ret_message))

        keys.append(ret_packet.ret_parameters.get('key'))

    return keys

def get_data_blocks(addresses, keys):
    client = FriClient()

    for key in keys:
        params = {'key': key, 'replica_count':2}
        packet_obj = FabnetPacketRequest(method='ClientGetData', parameters=params, sync=True)
        nodeaddr = random.choice(addresses)

        ret_packet = client.call_sync(nodeaddr, packet_obj)
        if ret_packet.ret_code != 0:
            raise Exception('ClientGetData failed on %s: %s'%(nodeaddr, ret_packet.ret_message))

        data = ret_packet.binary_data.data()
        if hashlib.sha1(data).hexdigest() != ret_packet.ret_parameters['checksum']:
            raise Exception('Data block checksum failed!')

def collect_topology_from_nodes(addresses):
    for address in addresses:
        p = subprocess.Popen(['/usr/bin/python', './fabnet/bin/fri-caller', 'TopologyCognition', address, '{"need_rebalance": 1}', 'async'])
        node_i = address.split(':')[-1]
        wait_topology(node_i, len(addresses))
        #os.system('python ./tests/topology_to_tgf /tmp/node_%s/fabnet_topology.db /tmp/fabnet_topology.%s-orig.tgf'%(node_i, len(ADDRESSES)))

def wait_topology(node_i, nodes_count):
    conn = None
    while True:
        try:
            db = '/tmp/node_%s/fabnet_topology.db'%node_i

            while not os.path.exists(db):
                print '%s not exists!'%db
                time.sleep(0.2)

            time.sleep(.5)
            conn = sqlite3.connect(db)
            curs = conn.cursor()
            curs.execute("SELECT count(node_address) FROM fabnet_nodes WHERE old_data=0")
            rows = curs.fetchall()
            print 'nodes discovered: %s'%rows[0][0]
            if int(rows[0][0]) != nodes_count:
                time.sleep(.5)
            else:
                break
        finally:
            if conn:
                curs.close()
                conn.close()

