#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.monitor.monitor_dbapi

@author Konstantin Andrusenko
@date July 14, 2013
"""
from fabnet.utils.logger import oper_logger as logger
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.utils.db_conn import DBOperationalException, DBEmptyResult
from fabnet.monitor.constants import UP, DOWN

class AbstractDBAPI:
    def __init__(self):
        self.__cache = {}

    def get_nodes_list(self, status):
        keys = []
        for key, item in self.__cache.items():
            if item[0] == status:
                keys.append(key)
        return keys

    def change_node_status(self, nodeaddr, status):
        if not nodeaddr in self.__cache:
            return
        self.__cache[nodeaddr][0] = status 

    def update_node_info(self, nodeaddr, node_name, home_dir, node_type, superior_neighbours, upper_neighbours):
        self.__cache[nodeaddr] = [UP, node_name, home_dir, node_type, superior_neighbours, upper_neighbours]

    def update_node_stat(self, nodeaddr, stat):
        pass

    def notification(self, notify_provider, notify_type, notify_topic, message, date):
        logger.info('[NOTIFICATION][%s][%s] *%s* %s'%(notify_type, notify_provider, notify_topic, message))

    def close(self):
        pass


class PostgresDBAPI(AbstractDBAPI): 
    def __init__(self, connect_string):
        self._conn = DBConnection(connect_string)

    def get_nodes_list(self, status):
        try:
            return self._conn.select_col("SELECT node_address FROM nodes_info WHERE status=%s", (status,))
        except DBEmptyResult:
            return []

    def change_node_status(self, nodeaddr, status):
        rows = self._conn.select("SELECT id FROM nodes_info WHERE node_address=%s", (nodeaddr, ))
        if not rows:
            self._conn.execute("INSERT INTO nodes_info (node_address, node_name, status) VALUES (%s, %s, %s)", (nodeaddr, '', status))
        else:
            self._conn.execute("UPDATE nodes_info SET status=%s WHERE id=%s", (status, rows[0][0]))

    def update_node_info(self, nodeaddr, node_name, home_dir, node_type, superior_neighbours, upper_neighbours):
        superiors = ','.join(superior_neighbours)
        uppers = ','.join(upper_neighbours)
        rows = self._conn.select("SELECT id, node_name, home_dir, node_type, status, superiors, uppers \
                                    FROM nodes_info WHERE node_address=%s", (nodeaddr, ))
        if not rows:
            self._conn.execute("INSERT INTO nodes_info (node_address, node_name, home_dir, node_type, status, superiors, uppers) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s)", (nodeaddr, node_name, home_dir, node_type, UP, superiors, uppers))
        else:
            if rows[0][1:] == (node_name, home_dir, node_type, UP, superiors, uppers):
                return
            self._conn.execute("UPDATE nodes_info \
                                SET node_name=%s, home_dir=%s, node_type=%s, status=%s, superiors=%s, uppers=%s \
                                WHERE id=%s", \
                                (node_name, home_dir, node_type, UP, superiors, uppers, rows[0][0]))

    def update_node_stat(self, nodeaddr, stat):
        self._conn.execute("UPDATE nodes_info SET status=%s, statistic=%s, last_check=%s \
                            WHERE node_address=%s", (UP, stat, datetime.now(), nodeaddr))

    def notification(self, notify_provider, notify_type, notify_topic, message, date):
        self._conn.execute("INSERT INTO notification (node_address, notify_type, notify_topic, \
                notify_msg, notify_dt) VALUES (%s, %s, %s, %s, %s)", \
                (notify_provider, notify_type, notify_topic, message, date))

    def close(self):
        self._conn.close()
