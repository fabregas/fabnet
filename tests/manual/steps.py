#!/usr/bin/python
# -*- coding: utf-8 -*-
from lettuce import *
import sys
import os
import re
import time
import random

import primitives
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.monitor.monitor_operator import MONITOR_DB


LOG_DIR = '/tmp/fabnet-test-logs'

#----------------------------------------------------------------------------------------

def get_log_file(feature, scenario):
    f_path = os.path.join(LOG_DIR, feature.replace(' ', '_').replace('/', '_'))
    if not os.path.exists(f_path):
        os.makedirs(f_path)

    f_path = os.path.join(f_path, scenario.replace(' ', '_').replace('/', '_'))
    return open(f_path, 'w')


@step(u'Given I have nodes with HDDs:(.+)')
def given_i_have_nodes_with_hdds(step, hdds_str):
    def to_bytes(s):
        return int(float(s.strip())*1024*1024)

    world.hdds = [to_bytes(s) for s in hdds_str.split(',')]

@step(u'When start network')
def when_start_network(step):
    world.addresses, world.processes = primitives.create_network('127.0.0.1', world.hdds)

@step(u'And put data until HDDs are full')
def and_put_data_until_hdds_are_full(step):
    while True:
        is_all_full = primitives.put_data_to_network(world.addresses, world.stat_f_obj)
        if is_all_full:
            break
        time.sleep(40)

@step(u'Then maximum HDD free space less than (\d+)')
def then_maximum_hdd_free_space_less_than(step, free_space):
    max_free_space, _, _ = primitives.get_maximum_free_space(world.addresses)
    if int(free_space) < max_free_space:
        raise Exception('Expected free space should be less than %s. but - %s occured'%(free_space, max_free_space))



@step(u'And put data until HDDs have free capacity less than (\d+)')
def and_put_data_until_hdds_have_free_capacity_less_than(step, free):
    primitives.put_data_to_network(world.addresses, world.stat_f_obj, int(free))

@step(u'Then wait check and repair operation finish with label:(.+)')
def then_wait_check_and_repair_operation_finish(step, label):
    world.stat_f_obj.write('\n\n#process RepairDataBlocks operation over %s...\n'%label.strip())
    primitives.call_repair_data(world.addresses[0], world.stat_f_obj, len(world.hdds), world.check_node_only)
    world.check_node_only = None


def parse_int_lst(nums_str):
    parts = nums_str.split(',')
    return [int(s.strip()) for s in parts]

@step(u'When destroy data from node with nums: (.+)')
def when_destroy_data_from_node_with_nums(step, nums_str):
    nums = parse_int_lst(nums_str)
    destroyed = 0
    for num in nums:
        destroyed += primitives.destroy_data(world.addresses[num], num)

    world.stat_f_obj.write('Destroyed %s Gb data...\n'%(destroyed/1024./1024/1024))

@step(u'And check range from node with num: (\d+)')
def and_check_range_from_node_with_nums(step, num):
    world.check_node_only = world.addresses[int(num)]


@step(u'And clear monitoring stat')
def and_clear_monitoring_stat(step):
    conn = DBConnection("dbname=%s user=postgres"%MONITOR_DB)
    conn.execute('DELETE FROM notification')
    conn.execute('DELETE FROM nodes_info')


@step(u'And wait (\d+) seconds')
def and_wait_n_seconds(step, secs):
    secs = int(secs)
    time.sleep(secs)

@step(u'Then see collected stats for all nodes')
def then_see_collected_stats_for_all_nodes(step):
    conn = DBConnection("dbname=%s user=postgres"%MONITOR_DB)
    qeury_nodes = 'SELECT node_address, node_name, status, superiors, uppers, statistic, last_check FROM nodes_info'
    nodes = conn.select(qeury_nodes)
    conn.close()
    if len(nodes) != len(world.processes):
        raise Exception('Expected %i nodes in nodes_info table. But %i occured!'%(len(world.addresses), len(nodes)))
    for node in nodes:
        for field in node:
            if not field:
                raise Exception('Invalid node info found: %s'%str(node))



@step(u'When start virtual network with (\d+) nodes')
def when_start_virtual_network(step, nodes_count):
    addresses, processes = primitives.create_virt_net(int(nodes_count), len(world.addresses))
    world.addresses += addresses
    world.processes += processes

@step(u'When stop (\d+) nodes')
def when_stop_n_nodes(step, nodes_count):
    stop_nodes = []
    stop_addrs = []
    for i in xrange(int(nodes_count)):
        while True:
            proc = random.choice(world.processes)
            if proc not in stop_nodes:
                stop_addrs.append(world.addresses[world.processes.index(proc)])
                stop_nodes.append(proc)
                break

    primitives.destroy_network(stop_nodes, destroy_hdds=False)

    for proc in stop_nodes:
        del world.processes[world.processes.index(proc)]

    for addr in stop_addrs:
        world.stat_f_obj.write('\nStopping %s node...\n'%addr)
        del world.addresses[world.addresses.index(addr)]

@step(u'Then I collect DHT statistic')
def then_i_collect_dht_statistic(step):
    primitives.print_ranges(world.addresses, world.stat_f_obj)

@step(u'Then I put (\d+) blocks \(one block size - (\d+) bytes\)')
def then_i_put_n_blocks_one_block_size_m_bytes(step, blocks, block_size):
    world.keys = primitives.put_data_blocks(world.addresses, int(block_size), int(blocks))


@step(u'Then I get and check all data blocks')
def then_i_get_and_check_all_data_blocks(step):
    primitives.get_data_blocks(world.addresses, world.keys)


@step(u'Then I collect topology from every node')
def then_i_collect_topology_from_every_node(step):
    primitives.collect_topology_from_nodes(world.addresses)

@before.each_scenario
def try_start_network(scenario):
    world.check_node_only = None
    world.hdds = []
    world.addresses = []
    world.processes = []
    world.stat_f_obj = get_log_file(scenario.feature.name, scenario.name)

@after.each_scenario
def try_stop_network(scenario):
    if world.processes:
        primitives.destroy_network(world.processes)

    if world.stat_f_obj:
        world.stat_f_obj.close()


