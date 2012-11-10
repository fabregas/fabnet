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
import sqlite3

from fabnet.core.operator import Operator

MONITOR_DB = 'monitor.db'

class MonitorOperator(Operator):
    OPTYPE = 'Monitor'

    def __init__(self, self_address, home_dir='/tmp/', certfile=None, is_init_node=False, node_name='unknown'):
        Operator.__init__(self, self_address, home_dir, certfile, is_init_node, node_name)

        self.__monitor_db_path = os.path.join(self.home_dir, MONITOR_DB)
        self._init_db()


    def _init_db(self):
        conn = sqlite3.connect(self.__monitor_db_path)

        curs = conn.cursor()
        #curs.execute("CREATE TABLE IF NOT EXISTS nodes_info (node_address TEXT, node_name TEXT, statistic TEXT, last_check DATETIME)")
        curs.execute("CREATE TABLE IF NOT EXISTS notification (node_address TEXT, notify_type TEXT, notify_msg TEXT, notify_dt DATETIME)")
        conn.commit()

        curs.close()
        conn.close()

    def on_network_notify(self, notify_type, notify_provider, message):
        """This method should be imlemented for some actions
            on received network nofitications
        """
        conn = sqlite3.connect(self.__monitor_db_path)

        curs = conn.cursor()
        curs.execute("INSERT INTO notification (node_address, notify_type, notify_msg, notify_dt) VALUES ('%s', '%s', '%s', '%s')"% \
                        (notify_provider, notify_type, message, datetime.now()))
        conn.commit()
        curs.close()
        conn.close()
