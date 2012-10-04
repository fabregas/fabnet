
from fabnet.core.operator import Operator
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition
from fabnet.operations.node_statistic import NodeStatisticOperation

from fabnet.dht_mgmt.dht_operator import DHTOperator
from fabnet.dht_mgmt.operations.get_range_data_request import GetRangeDataRequestOperation
from fabnet.dht_mgmt.operations.get_ranges_table import GetRangesTableOperation
from fabnet.dht_mgmt.operations.put_data_block import PutDataBlockOperation
from fabnet.dht_mgmt.operations.get_data_block import GetDataBlockOperation
from fabnet.dht_mgmt.operations.split_range_cancel import SplitRangeCancelOperation
from fabnet.dht_mgmt.operations.split_range_request import SplitRangeRequestOperation
from fabnet.dht_mgmt.operations.update_hash_range_table import UpdateHashRangeTableOperation
from fabnet.dht_mgmt.operations.check_hash_range_table import CheckHashRangeTableOperation
from fabnet.dht_mgmt.operations.client_get import ClientGetOperation
from fabnet.dht_mgmt.operations.client_put import ClientPutOperation

OPERATOR = DHTOperator

OPERATIONS_MAP = {'ManageNeighbour': ManageNeighbour,
                    'DiscoveryOperation': DiscoveryOperation,
                    'TopologyCognition': TopologyCognition,
                    'NodeStatistic': NodeStatisticOperation,
                        'GetRangeDataRequest': GetRangeDataRequestOperation,
                        'GetRangesTable': GetRangesTableOperation,
                        'PutDataBlock': PutDataBlockOperation,
                        'GetDataBlock': GetDataBlockOperation,
                        'SplitRangeCancel': SplitRangeCancelOperation,
                        'SplitRangeRequest': SplitRangeRequestOperation,
                        'UpdateHashRangeTable': UpdateHashRangeTableOperation,
                        'CheckHashRangeTable': CheckHashRangeTableOperation,
                        'ClientGetData': ClientGetOperation,
                        'ClientPutData': ClientPutOperation}
