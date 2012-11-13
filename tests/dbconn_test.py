import unittest
import time
import os
import logging
import threading
import json
import random

from fabnet.utils import db_conn
from fabnet.utils.logger import logger


class TestDBConn(unittest.TestCase):
    def test_dbconn(self):
        os.system('rm /tmp/test_dbconn.db')
        db = db_conn.SqliteDBConnection('/tmp/test_dbconn.db')
        db.execute("CREATE TABLE test_table (ID NUMBER, NAME TEXT, DESCR TEXT)")
        try:
            db.execute("CREATE TABLE test_table (ID NUMBER, NAME TEXT, DESCR TEXT)")
        except db_conn.DBOperationalException:
            pass
        else:
            raise Exception('should be exception in this case')

        row_id = db.execute("INSERT INTO test_table (ID,NAME,DESCR) VALUES (1, ?, ?)", ('Fabregas', 'yet another developer'))
        self.assertTrue(row_id is not None)

        query = "SELECT ID,NAME,DESCR FROM test_table"
        rows = db.select(query)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[0][1], 'Fabregas')
        self.assertEqual(rows[0][2], 'yet another developer')

        rows = db.select_one(query)
        self.assertEqual(rows, 1)

        rows = db.select_row(query)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], 1)
        self.assertEqual(rows[1], 'Fabregas')
        self.assertEqual(rows[2], 'yet another developer')

        rows = db.select_col(query)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], 1)

        db.close()


if __name__ == '__main__':
    unittest.main()

