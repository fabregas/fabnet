
from fabnet.core.operator import Operator
from fabnet.dht_mgmt.dht_operator import DHTOperator
from fabnet.monitor.monitor_operator import MonitorOperator

OPERATORS_MAP = {'BASE': Operator, 'DHT': DHTOperator, 'Monitor': MonitorOperator}
DEFAULT_OPERATOR= Operator
