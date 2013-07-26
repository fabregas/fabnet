import unittest
import time
import os
import logging
import threading
import json
import random
import base64
from fabnet.core import constants
from fabnet.utils.logger import logger
from fabnet.mgmt.mgmt_db import MgmtDatabaseManager
from fabnet.mgmt.management_engine_api import ManagementEngineAPI
from fabnet.mgmt.exceptions import *
from fabnet.mgmt.constants import *

from pymongo import MongoClient

logger.setLevel(logging.DEBUG)

KS_PATH = './tests/cert/admin_ks.zip'
KS_PASSWD = 'qwerty123'

class TestManagementEngineAPI(unittest.TestCase):
    def test00_init(self):
        with self.assertRaises(MEDatabaseException):
            dbm = MgmtDatabaseManager('some-host-name')

        cl = MongoClient('localhost')
        cl.drop_database('test_fabnet_mgmt_db')
        MgmtDatabaseManager.MGMT_DB_NAME = 'test_fabnet_mgmt_db'

        dbm = MgmtDatabaseManager('localhost')
        mgmt_api = ManagementEngineAPI(dbm)

        with self.assertRaises(MEAuthException):
            mgmt_api.get_cluster_config(None)

        is_init = mgmt_api.is_initialized()
        self.assertEqual(is_init, True)
        
        with self.assertRaises(MEAuthException):
            mgmt_api.authenticate('test', 'test')
        with self.assertRaises(MEAuthException):
            mgmt_api.authenticate('admin', 'test')

        session_id = mgmt_api.authenticate('admin', 'admin')
        
        methods = mgmt_api.get_allowed_methods(session_id)
        self.assertTrue('create_user' in methods)

        user_info = mgmt_api.get_user_info(session_id, 'admin')
        self.assertEqual(user_info[DBK_USERNAME], 'admin')
        self.assertEqual(user_info[DBK_ROLES], [ROLE_UM])

        mgmt_api.change_user_password(session_id, None, 'qwerty')
        mgmt_api.create_user(session_id, 'megaadmin', 'qwerty', \
                [ROLE_RO, ROLE_CF, ROLE_SS, ROLE_UPGR, ROLE_UM])
        mgmt_api.change_user_password(session_id, 'megaadmin', 'qwerty')

        mgmt_api.change_user_roles(session_id, 'megaadmin', [ROLE_RO, ROLE_CF, ROLE_UPGR])

        mgmt_api.logout(session_id)
        with self.assertRaises(MEAuthException):
            mgmt_api.get_cluster_config(session_id)

        with self.assertRaises(MEAuthException):
            mgmt_api.authenticate('admin', 'admin')

        session_id = mgmt_api.authenticate('admin', 'qwerty')

        roles = mgmt_api.get_available_roles(session_id)
        self.assertEqual(type(roles), dict)
        self.assertTrue(ROLE_SS in roles)

        ma_session_id = mgmt_api.authenticate('megaadmin', 'qwerty')

        with self.assertRaises(MEPermException):
            mgmt_api.change_user_password(ma_session_id, 'admin', 'qwerty')

        with self.assertRaises(MEPermException):
            mgmt_api.create_user(ma_session_id, 'rouser', 'qwerty', [ROLE_RO])

        with self.assertRaises(MEInvalidArgException):
            mgmt_api.create_user(session_id, 'rouser', 'qwerty', ['ooops'])
        mgmt_api.create_user(session_id, 'rouser', 'qwerty', [ROLE_RO])
        with self.assertRaises(MEAlreadyExistsException):
            mgmt_api.create_user(session_id, 'rouser', 'qwerty', [ROLE_RO])

        with self.assertRaises(MENotFoundException):
            mgmt_api.change_user_roles(session_id, 'someuser', [ROLE_RO, ROLE_SS])

        with self.assertRaises(MEInvalidArgException):
            mgmt_api.create_user(session_id, 'test', 'dd', ROLE_RO)

        with self.assertRaises(MEInvalidArgException):
            mgmt_api.create_user(session_id, '', 'dd', [ROLE_RO])

        with self.assertRaises(MEInvalidArgException):
            mgmt_api.change_user_roles(session_id, 'rouser', ROLE_RO)
        
        mgmt_api.remove_user(session_id, 'rouser')
        with self.assertRaises(MEAuthException):
            mgmt_api.authenticate('rouser', 'qwerty')
        mgmt_api.logout(session_id)

        config = mgmt_api.get_cluster_config(ma_session_id)
        self.assertEqual(config, {})
        config = {DBK_CONFIG_CLNAME: 'testcluster',
                  DBK_CONFIG_KS: base64.b64encode(open(KS_PATH, 'rb').read())}
        mgmt_api.configure_cluster(ma_session_id, config)

        mgmt_api.logout(ma_session_id)

    def test01_operations(self):
        dbm = MgmtDatabaseManager('localhost')
        mgmt_api = ManagementEngineAPI(dbm)

        is_init = mgmt_api.is_initialized()
        self.assertEqual(is_init, False)

        mgmt_api.initialize(KS_PASSWD)
        
        is_init = mgmt_api.is_initialized()
        self.assertEqual(is_init, True)

if __name__ == '__main__':
    unittest.main()

