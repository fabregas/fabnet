#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.operator.neighbours_discovery
@author Konstantin Andrusenko
@date January 6, 2013

This module contains the NeigboursDiscoveryRoutines class implementation
"""
import os
from fabnet.utils.logger import oper_logger as logger
from fabnet.utils.db_conn import SqliteDBConnection as DBConnection
from fabnet.operations.topology_cognition import TOPOLOGY_DB
from fabnet.operations.constants import MNO_APPEND, MNO_REMOVE
from fabnet.core.constants import ONE_DIRECT_NEIGHBOURS_COUNT,\
                                        NT_SUPERIOR, NT_UPPER


class NeigboursDiscoveryRoutines:
    def __init__(self, operator):
        self.operator = operator

        self.__discovery_cache = {}
        self.__new_upper = None
        self.__new_superior = None

        #manage neigbours cached obj
        self.__cache = {}
        self.__cache[NT_UPPER] = []
        self.__cache[NT_SUPERIOR] = []
        self.__discovered_nodes = {}
        self.__discovered_nodes[NT_UPPER] = []
        self.__discovered_nodes[NT_SUPERIOR] = []

        self.__balanced = False

    def discovery_nodes(self, node, uppers, superiors):
        self.__discovery_cache[node] = (uppers, superiors)

        interset_nodes = list(set(superiors) & set(uppers))
        interset = None
        for interset in interset_nodes:
            if interset not in self.__discovery_cache:
                continue

            int_uppers, int_superiors = self.__discovery_cache[interset]

            if not self.__new_superior:
                if len(uppers) > ONE_DIRECT_NEIGHBOURS_COUNT:
                    self.__new_superior = interset
                elif len(int_uppers) > ONE_DIRECT_NEIGHBOURS_COUNT:
                    self.__new_superior = node

            if len(superiors) > ONE_DIRECT_NEIGHBOURS_COUNT:
                self.__new_upper = interset
            elif len(int_superiors) > ONE_DIRECT_NEIGHBOURS_COUNT:
                self.__new_upper = node

            if self.__new_upper is None and self.__new_superior is None:
                self.__new_upper = node
                #self.__new_superior = interset

            if self.__new_upper and self.__new_superior:
                self._manage_new_neighbours()
                return

            break #one neighbour found
        else:
            for interset in interset_nodes:
                if interset not in self.__discovery_cache:
                    #call discovery
                    self.operator.async_remote_call(interset, 'DiscoveryOperation', {})
                    return

        for superior in superiors:
            if superior in self.__discovery_cache:
                continue
            #call discovery next...
            self.operator.async_remote_call(superior, 'DiscoveryOperation', {})
            return

        for node, (uppers, superiors) in self.__discovery_cache.items():
            if node in (self.__new_upper, self.__new_superior):
                continue
            if not self.__new_upper and len(uppers) > ONE_DIRECT_NEIGHBOURS_COUNT:
                self.__new_upper = uppers[0]
            elif not self.__new_superior and len(superiors) > ONE_DIRECT_NEIGHBOURS_COUNT:
                self.__new_superior = superiors[0]

            if self.__new_superior and self.__new_upper:
                break
        else:
            for node, (uppers, superiors) in self.__discovery_cache.items():
                if node in (self.__new_upper, self.__new_superior):
                    continue
                if not self.__new_upper and len(superiors) <= ONE_DIRECT_NEIGHBOURS_COUNT:
                    self.__new_upper = node
                elif not self.__new_superior and len(uppers) <= ONE_DIRECT_NEIGHBOURS_COUNT:
                    self.__new_superior = node

                if self.__new_superior and self.__new_upper:
                    break
            else:
                if self.__new_superior:
                    self.__new_upper = self.__new_superior
                else:
                    self.__new_superior = self.__new_upper

        #send ManageNeighbour request
        self._manage_new_neighbours()

    def _manage_new_neighbours(self):
        logger.info('Discovered neigbours: %s and %s'%(self.__new_superior, self.__new_upper))

        parameters = { 'neighbour_type': NT_SUPERIOR, 'operation': MNO_APPEND,
                        'node_address': self.operator.self_address, 'operator_type': self.operator.OPTYPE }
        self.operator.async_remote_call(self.__new_superior, 'ManageNeighbour', parameters)

        parameters = { 'neighbour_type': NT_UPPER, 'operation': MNO_APPEND,
                        'node_address': self.operator.self_address, 'operator_type': self.operator.OPTYPE }
        self.operator.async_remote_call(self.__new_upper, 'ManageNeighbour', parameters)

    def process_manage_neighbours(self, n_type, operation, node_address, op_type, is_force):
        ret_params = {}
        neighbours = self.operator.get_neighbours(n_type)
        if operation == MNO_APPEND:
            if op_type is None:
                raise Exception('Operator type parameter is expected for ManageNeighbour operation')

            if len(neighbours) >= (ONE_DIRECT_NEIGHBOURS_COUNT+1):
                ret_params['dont_append'] = True
            else:
                self.operator.set_neighbour(n_type, node_address, op_type)

            self.__discovered_nodes[n_type].append(node_address)
        elif operation == MNO_REMOVE:
            if (len(neighbours)  > ONE_DIRECT_NEIGHBOURS_COUNT) or is_force:
                self.operator.remove_neighbour(n_type, node_address)
            else:
                ret_params['dont_remove'] = True

        if is_force:
            self.__discovered_nodes[n_type].append(node_address)
            return

        if operation == MNO_APPEND:
            r_neighbours = self.operator.get_neighbours(NT_UPPER) + \
                                        self.operator.get_neighbours(NT_SUPERIOR)
            ret_params['neighbours'] = dict(zip(r_neighbours, [0 for i in r_neighbours])).keys()

        if n_type == NT_SUPERIOR:
            n_type = NT_UPPER
        elif n_type == NT_UPPER:
            n_type = NT_SUPERIOR

        ret_params['node_address'] = self.operator.self_address
        ret_params['neighbour_type'] = n_type
        ret_params['operator_type'] = self.operator.OPTYPE
        return ret_params

    def callback_manage_neighbours(self, n_type, operation, node_address, op_type, dont_append, dont_remove):
        if operation == MNO_APPEND:
            if op_type is None:
                raise Exception('Operator type parameter is expected for ManageNeighbour operation')
            self.__discovered_nodes[n_type].append(node_address)
            if not dont_append:
                self.operator.set_neighbour(n_type, node_address, op_type)

        elif operation == MNO_REMOVE:
            if not dont_remove:
                self.operator.remove_neighbour(n_type, node_address)
            else:
                self.__cache[n_type].append(node_address)

    def _check_neighbours_count(self, n_type, neighbours, other_n_type, other_neighbours, ret_parameters):
        if len(neighbours) >= ONE_DIRECT_NEIGHBOURS_COUNT:
            self.__discovered_nodes[n_type] = []
            return

        new_node = None
        for node in ret_parameters.get('neighbours', []):
            #trying find new node...
            if (node in self.__discovered_nodes[n_type]) or (node in neighbours) \
                    or (node in other_neighbours) or (node == self.operator.self_address):
                continue
            new_node = node
            break

        d_nodes = self.get_discovered_nodes()
        for node_addr, node_info in d_nodes.items():
            if (node_addr in self.__discovered_nodes[n_type]) or (node_addr in neighbours) \
                    or (node_addr in other_neighbours) or (node_addr == self.operator.self_address):
                continue
            new_node = node_addr
            if len(set(node_info[0]) & set(node_info[1])):
                #node with interset neighbours
                break

        if new_node == None:
            for node in neighbours:
                if node not in self.__discovered_nodes[n_type]:
                    new_node = node
                    break

        if new_node == None:
            for node in other_neighbours:
                if node not in self.__discovered_nodes[n_type]:
                    new_node = node
                    break

        if new_node is None:
            return

        parameters = { 'neighbour_type': other_n_type, 'operation': MNO_APPEND,
                    'node_address': self.operator.self_address, 'operator_type': self.operator.OPTYPE }
        self.operator.async_remote_call(new_node, 'ManageNeighbour', parameters)
        self.__discovered_nodes[n_type].append(new_node)


    def rebalance_append(self, ret_parameters, reinit_discovery=False):
        upper_neighbours = self.operator.get_neighbours(NT_UPPER)
        superior_neighbours = self.operator.get_neighbours(NT_SUPERIOR)

        if reinit_discovery:
            self.__discovered_nodes[ret_parameters['neighbour_type']] = []

        if ret_parameters['neighbour_type'] == NT_UPPER:
            self._check_neighbours_count(NT_UPPER, upper_neighbours, NT_SUPERIOR, superior_neighbours, ret_parameters)
        else:
            self._check_neighbours_count(NT_SUPERIOR, superior_neighbours, NT_UPPER, upper_neighbours, ret_parameters)


    def _get_for_delete(self, neighbours, n_type, other_neighbours):
        if len(neighbours) <= ONE_DIRECT_NEIGHBOURS_COUNT:
            return

        intersec_neighbours = list(set(neighbours) & set(other_neighbours))

        for neighbour in intersec_neighbours:
            if neighbour in self.__cache[n_type]:
                continue

            self.__cache[n_type].append(neighbour)
            return neighbour

        rest_neighbours = list(set(neighbours) ^ set(intersec_neighbours))
        for neighbour in rest_neighbours:
            if neighbour in self.__cache[n_type]:
                continue
            self.__cache[n_type].append(neighbour)
            return neighbour

        return None


    def rebalance_remove(self):
        upper_neighbours = self.operator.get_neighbours(NT_UPPER)
        superior_neighbours = self.operator.get_neighbours(NT_SUPERIOR)


        parameters = {'operation': MNO_REMOVE, 'node_address': self.operator.self_address, \
                        'operator_type': self.operator.OPTYPE}

        for_delete = self._get_for_delete(superior_neighbours, NT_SUPERIOR, upper_neighbours)
        if for_delete:
            parameters['neighbour_type'] = NT_UPPER
        else:
            for_delete = self._get_for_delete(superior_neighbours, NT_UPPER, upper_neighbours)
            parameters['neighbour_type'] = NT_SUPERIOR


        if for_delete:
            self.operator.async_remote_call(for_delete, 'ManageNeighbour', parameters)
        else:
            self.__cache[NT_UPPER] = []
            self.__cache[NT_SUPERIOR] = []


    def get_discovered_nodes(self):
        db = os.path.join(self.operator.home_dir, TOPOLOGY_DB)
        if not os.path.exists(db):
            return {}
        conn = DBConnection(db)
        rows = conn.select("SELECT node_address, superiors, uppers, old_data FROM fabnet_nodes")
        conn.close()

        nodes = {}
        for row in rows:
            nodes[row[0]] = (row[1].split(','), row[2].split(','), int(row[3]))

        return nodes

    def smart_neighbours_rebalance(self, node_address, superior_neighbours, upper_neighbours):
        if node_address == self.operator.self_address:
            return

        uppers = self.operator.get_neighbours(NT_UPPER)
        superiors = self.operator.get_neighbours(NT_SUPERIOR)
        if (node_address in uppers) or (node_address in superiors):
            return

        if ONE_DIRECT_NEIGHBOURS_COUNT > len(superiors) >= (ONE_DIRECT_NEIGHBOURS_COUNT+1):
            return

        intersec_count = len(set(uppers) & set(superiors))
        if intersec_count == 0:
            #good neighbours connections
            return

        intersec_count = len(set(superior_neighbours) & set(upper_neighbours))
        if intersec_count > 0 and (len(upper_neighbours) <= ONE_DIRECT_NEIGHBOURS_COUNT):
            parameters = { 'neighbour_type': NT_UPPER, 'operation': MNO_APPEND,
                            'node_address': self.operator.self_address,
                            'operator_type': self.operator.OPTYPE }
            self.operator.async_remote_call(node_address, 'ManageNeighbour', parameters)

