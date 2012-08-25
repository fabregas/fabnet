#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations
@author Konstantin Andrusenko
@date September 7, 2012
"""
from .manage_neighbours import ManageNeighbour
from .discovery_operation import DiscoveryOperation
from .topology_cognition import TopologyCognition

OPERATIONS_MAP = {'ManageNeighbour': ManageNeighbour,
                    'DiscoveryOperation': DiscoveryOperation,
                    'TopologyCognition': TopologyCognition}
