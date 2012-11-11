#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.monitor.monitor_operator

@author Konstantin Andrusenko
@date November 11, 2012
"""
import os
import threading
from datetime import datetime

from fabnet.utils.db_conn import DBConnection
from fabnet.core.operator import Operator
from fabnet.utils.logger import logger

MONITOR_DB = 'monitor.db'

class MonitorOperator(Operator):
    OPTYPE = 'Monitor'

    def __init__(self, self_address, home_dir='/tmp/', certfile=None, is_init_node=False, node_name='unknown'):
        Operator.__init__(self, self_address, home_dir, certfile, is_init_node, node_name)

        self.__monitor_db_path = os.path.join(self.home_dir, MONITOR_DB)
        self._init_db()

    def _init_db(self):
        conn = DBConnection(self.__monitor_db_path)

        #curs.execute("CREATE TABLE IF NOT EXISTS nodes_info (node_address TEXT, node_name TEXT, statistic TEXT, last_check DATETIME)")
        conn.execute("CREATE TABLE IF NOT EXISTS notification (node_address TEXT, notify_type TEXT, notify_topic TEXT, notify_msg TEXT, notify_dt DATETIME)")

        conn.close()

    def on_network_notify(self, notify_type, notify_provider, notify_topic, message):
        """This method should be imlemented for some actions
            on received network nofitications
        """
        conn = DBConnection(self.__monitor_db_path)
        try:

            logger.info('[NOTIFICATION][%s][%s] *%s* %s'%(notify_type, notify_provider, notify_topic, message))
            conn.execute("INSERT INTO notification (node_address, notify_type, notify_topic, notify_msg, notify_dt) VALUES (?, ?, ?, ?, ?)", \
                        (notify_provider, notify_type, notify_topic, message, datetime.now()))
        finally:
            conn.close()


