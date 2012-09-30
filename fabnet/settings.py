
from fabnet.core.operator import Operator
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.operations.node_statistic import NodeStatisticOperation

OPERATOR = Operator

OPERATIONS_MAP = {'ManageNeighbour': ManageNeighbour,
                    'DiscoveryOperation': DiscoveryOperation,
                    'TopologyCognition': TopologyCognition,
                    'NodeStatistic': NodeStatisticOperation}
