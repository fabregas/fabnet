#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.mgmt.mgmt_db
@author Konstantin Andrusenko
@date July 24, 2013

This module contains the implementation of MgmtDatabaseManager class
"""

import hashlib
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from fabnet.mgmt.exceptions import *
from fabnet.mgmt.constants import *

class MgmtDatabaseManager:
    MGMT_DB_NAME = 'fabnet_mgmt_db'

    def __init__(self, conn_str):
        try:
            self.__client = MongoClient(conn_str)
        except ConnectionFailure, err:
            raise MEDatabaseException('No database connection! Details: %s'%err)
        self.__mgmt_db = self.__client[self.MGMT_DB_NAME]
        self.__check_users()

    def __check_users(self):
        users_cnt = self.__mgmt_db[DBK_USERS].find({}).count()
        if users_cnt:
            return
        self.create_user('admin', hashlib.sha1('admin').hexdigest(), [ROLE_UM])

    def close(self):
        self.__client.close()

    def get_cluster_config(self):
        config = self.__mgmt_db[DBK_CLUSTER_CONFIG].find_one({})
        if not config:
            return {}
        return config

    def set_cluster_config(self, config):
        old_config = self.__mgmt_db[DBK_CLUSTER_CONFIG].find_one({})
        if old_config:
            old_config.update(config)
            config = old_config

        self.__mgmt_db[DBK_CLUSTER_CONFIG].update({}, config, upsert=True)

    def get_user_info(self, username):
        user = self.__mgmt_db[DBK_USERS].find_one({DBK_USERNAME: username})
        return user

    def __validate(self, value, c_type, minlen=None, val_name=None, possible_vals=None):
        if not val_name:
            val_name = value

        if not isinstance(value, c_type):
            raise MEInvalidArgException('"%s" should be an instance of %s (%s occured)'\
                    %(val_name, c_type, type(value)))

        if minlen and len(value) < minlen:
            raise MEInvalidArgException('len(%s) < %s raised'%(val_name, minlen))

        if possible_vals:
            if type(value) not in (list, tuple):
                value = [value]
            for item in value:
                if item not in possible_vals:
                    raise MEInvalidArgException('"%s" does not supported! possible values: %s'\
                            %(item, possible_vals))

    def create_user(self, username, pwd_hash, roles):
        user = self.get_user_info(username)
        if user:
            raise MEAlreadyExistsException('User "%s" is already exists'%username)

        self.__validate(username, str, minlen=3, val_name='user_name')
        self.__validate(pwd_hash, str, minlen=1, val_name='password_hash')
        self.__validate(roles, list, minlen=1, val_name='roles', possible_vals=ROLES_DESC.keys())

        user = {DBK_USERNAME: username,
                     DBK_USER_PWD_HASH: pwd_hash,
                     DBK_ROLES: roles}
        self.__mgmt_db[DBK_USERS].insert(user)

    def remove_user(self, username):
        self.__mgmt_db[DBK_USERS].remove({DBK_USERNAME: username})

    def update_user_info(self, username, pwd_hash=None, roles=None):
        user = self.__mgmt_db[DBK_USERS].find_one({DBK_USERNAME: username})
        if not user:
            raise MENotFoundException('User "%s" does not found!'%username)

        if pwd_hash:
            self.__validate(pwd_hash, str, minlen=1, val_name='password_hash')
            user[DBK_USER_PWD_HASH] = pwd_hash

        if roles:
            self.__validate(roles, list, minlen=1, val_name='roles', possible_vals=ROLES_DESC.keys())
            user[DBK_ROLES] = roles

        self.__mgmt_db[DBK_USERS].update({DBK_USERNAME: username}, user)


    def add_session(self, session_id, username):
        self.__mgmt_db[DBK_SESSIONS].insert({DBK_ID: session_id, \
                                        DBK_USERNAME: username, \
                                        DBK_START_DT: datetime.now()})
            
    def del_session(self, session_id):
        self.__mgmt_db[DBK_SESSIONS].remove({DBK_ID: session_id})

    def get_user_by_session(self, session_id):
        session = self.__mgmt_db[DBK_SESSIONS].find_one({DBK_ID: session_id})
        if not session:
            return None
        username = session[DBK_USERNAME]
        user = self.get_user_info(username)
        if not user:
            return None
        return user

    def get_user_last_session(self, username):
        sessions = self.__mgmt_db[DBK_SESSIONS].find({DBK_USERNAME: username}).sort([(DBK_START_DT, -1)])
        for session in sessions:
            return session
        return None

