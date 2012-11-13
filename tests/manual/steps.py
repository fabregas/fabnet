#!/usr/bin/python
# -*- coding: utf-8 -*-
from lettuce import *
import sys
import os
import re
import time

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
    world.stat_f_obj = get_log_file(step.scenario.feature.name, step.scenario.name)

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
    if len(nodes) != len(world.addresses):
        raise Exception('Expected %i nodes in nodes_info table. But %i occured!'%(len(world.addresses), len(nodes)))
    for node in nodes:
        for field in node:
            if not field:
                raise Exception('Invalid node info found: %s'%str(node))


@before.each_scenario
def try_start_network(feature):
    world.check_node_only = None
    world.hdds = []
    world.addresses = []
    world.processes = []
    world.stat_f_obj = None

@after.each_scenario
def try_stop_network(feature):
    if world.processes:
        primitives.destroy_network(world.processes)

    if world.stat_f_obj:
        world.stat_f_obj.close()


