#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.monitor.notify_operation_mon

@author Konstantin Andrusenko
@date January 10, 2012
"""
import os
from datetime import datetime

from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE, ET_INFO, ET_ALERT
from fabnet.operations.notify_operation import NotifyOperation
from fabnet.monitor.constants import MONITOR_DB, UP, DOWN

class NotifyOperationMon(NotifyOperation):
    ROLES = [NODE_ROLE]
    NAME = 'NotifyOperation'

    def init_locals(self):
        self.__monitor_db_path = "dbname=%s user=postgres"%MONITOR_DB
        self._conn = DBConnection(self.__monitor_db_path)

    def on_network_notify(self, notify_type, notify_provider, notify_topic, message):
        logger.info('[NOTIFICATION][%s][%s] *%s* %s'%(notify_type, notify_provider, notify_topic, message))
        self._conn.execute("INSERT INTO notification (node_address, notify_type, notify_topic, notify_msg, notify_dt) VALUES (%s, %s, %s, %s, %s)", \
                    (notify_provider, notify_type, notify_topic, message, datetime.now()))

        if notify_topic == 'NodeUp':
            self.operator.change_node_status(notify_provider, UP)
        elif notify_topic == 'NodeDown':
            self.operator.change_node_status(notify_provider, DOWN)
