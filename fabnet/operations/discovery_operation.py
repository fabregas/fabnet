#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.discovery_operation

@author Konstantin Andrusenko
@date September 7, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.operations.constants import MNO_APPEND
from fabnet.core.constants import NT_SUPERIOR, NT_UPPER, \
                        ONE_DIRECT_NEIGHBOURS_COUNT
from fabnet.utils.logger import logger

class DiscoveryOperation(OperationBase):
    def __init__(self, operator):
        OperationBase.__init__(self, operator)
        self.__discovery_cache = {}
        self.__new_upper = None
        self.__new_superior = None


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        uppers = self.operator.get_neighbours(NT_UPPER)
        superiors = self.operator.get_neighbours(NT_SUPERIOR)
        return FabnetPacketResponse(ret_parameters={'uppers': uppers, \
                'superiors': superiors, 'node': self.operator.self_address})

    def callback(self, packet, sender=None):
        """In this method should be implemented logic of processing
        response packet from requested node

        @param packet - object of FabnetPacketResponse class
        @param sender - address of sender node.
        If sender == None then current node is operation initiator
        @return object of FabnetPacketResponse
                that should be resended to current node requestor
                or None for disabling packet resending
        """
        node = packet.ret_parameters['node']
        uppers = packet.ret_parameters.get('uppers', [])
        superiors = packet.ret_parameters.get('superiors', [])

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
                    self._init_operation(interset, 'DiscoveryOperation', {})
                    return


        for superior in superiors:
            if superior in self.__discovery_cache:
                continue
            #call discovery next...
            self._init_operation(superior, 'DiscoveryOperation', {})
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
                        'node_address': self.operator.self_address }
        self._init_operation(self.__new_superior, 'ManageNeighbour', parameters)

        parameters = { 'neighbour_type': NT_UPPER, 'operation': MNO_APPEND,
                        'node_address': self.operator.self_address }
        self._init_operation(self.__new_upper, 'ManageNeighbour', parameters)
