#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.db_conn
@author Konstantin Andrusenko
@date November 12, 2012
"""
import threading


class DBException(Exception):
    pass

class DBConnectionException(DBException):
    pass

class DBOperationalException(DBException):
    pass

class DBEmptyResult(DBOperationalException):
    pass


class AbstractDBConnection:
    def __init__(self, conn_string):
        self._conn_string = conn_string
        self._conn = None
        self.__lock = threading.Lock()

    def connect(self):
        raise Exception('Not implemented')

    def select(self, query, params=[]):
        self.__lock.acquire()
        try:
            if not self._conn:
                self.connect()

            curs = self._conn.cursor()
            try:
                curs.execute(query, params)
                return curs.fetchall()
            except Exception, err:
                self._conn.rollback()
                raise DBOperationalException(err)
            finally:
                curs.close()
        finally:
            self.__lock.release()

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
        self.__lock.acquire()
        try:
            if not self._conn:
                self.connect()

            curs = self._conn.cursor()
            try:
                curs.execute(query, params)
                self._conn.commit()
                return curs.lastrowid
            except Exception, err:
                self._conn.rollback()
                raise DBOperationalException(err)
            finally:
                curs.close()
        finally:
            self.__lock.release()


    def close(self):
        if self._conn:
            self._conn.close()

        self._conn = None

class SqliteDBConnection(AbstractDBConnection):
    def connect(self):
        try:
            import sqlite3
            self._conn = sqlite3.connect(self._conn_string)
        except Exception, err: #FIXME
            raise DBConnectionException(err)

class PostgresqlDBConnection(AbstractDBConnection):
    def connect(self):
        try:
            import psycopg2
            self._conn = psycopg2.connect(self._conn_string)
        except Exception, err: #FIXME
            raise DBConnectionException(err)

