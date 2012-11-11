#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.db_conn
@author Konstantin Andrusenko
@date November 12, 2012
"""

import sqlite3

class DBException(Exception):
    pass

class DBConnectionException(DBException):
    pass

class DBOperationalException(DBException):
    pass

class DBEmptyResult(DBOperationalException):
    pass

class DBConnection:
    def __init__(self, conn_string):
        self.__conn_string = conn_string
        self.__conn = None

    def connect(self):
        try:
            self.__conn = sqlite3.connect(self.__conn_string)
        except Exception, err: #FIXME
            raise DBConnectionException(err)

    def select(self, query, params=[]):
        if not self.__conn:
            self.connect()

        curs = self.__conn.cursor()
        try:
            curs.execute(query, params)
            return curs.fetchall()
        finally:
            curs.close()


    def select_one(self, query, params=[]):
        rows = self.select(query, params)
        if not rows:
            raise DBEmptyResult()
        return rows[0][0]

    def select_row(self, query, params=[]):
        rows = self.select(query, params)
        if not rows:
            raise DBEmptyResult()
        return rows[0]

    def select_col(self, query, params=[]):
        rows = self.select(query, params)
        if not rows:
            raise DBEmptyResult()
        return [r[0] for r in rows]

    def execute(self, query, params=[]):
        if not self.__conn:
            self.connect()

        curs = self.__conn.cursor()
        try:
            curs.execute(query, params)
            self.__conn.commit()
            return curs.lastrowid
        except Exception, err:
            self.__conn.rollback()
            raise DBOperationalException(err)
        finally:
            curs.close()


    def close(self):
        if self.__conn:
            self.__conn.close()

        self.__conn = None

