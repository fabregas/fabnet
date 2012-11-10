
from fabnet.dht_mgmt.dht_operator import DHTOperator
from fabnet.monitor.monitor_operator import MonitorOperator

OPERATOR = DHTOperator

OPERATORS_MAP = {'DHT': DHTOperator,
                'Monitor': MonitorOperator}

DEFAULT_OPERATOR = DHTOperator
