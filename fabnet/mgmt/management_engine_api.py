#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.mgmt.management_engine_api
@author Konstantin Andrusenko
@date July 24, 2013

This module contains the implementation of ManagementEngineAPI class
"""
import os
import hashlib
import tempfile 
import uuid
import base64

from fabnet.mgmt.constants import *
from fabnet.mgmt.exceptions import *
from fabnet.core.key_storage import AbstractKeyStorage

class ManagementEngineAPI:
    def __init__(self, db_mgr):
        self._db_mgr = db_mgr
        self._admin_ks = None
        self.__admin_ks_path = self.__get_admin_ks_path()

    def __del__(self):
        if self.__admin_ks_path:
            os.remove(self.__admin_ks_path)
        if self._db_mgr:
            self._db_mgr.close()

    def is_initialized(self):
        if not self.__admin_ks_path:
            return True
        return self._admin_ks is not None

    def initialize(self, ks_pwd):
        if not self.__admin_ks_path:
            return
        self._admin_ks = AbstractKeyStorage(self.__admin_ks_path, ks_pwd)

    def __get_admin_ks_path(self):
        config = self._db_mgr.get_cluster_config()
        ks_content = config.get(DBK_CONFIG_KS, None)
        if not ks_content:
            return None

        f_hdl, f_path = tempfile.mkstemp('-admin-ks') 
        ks_content = base64.b64decode(ks_content)
        os.write(f_hdl, ks_content)
        os.close(f_hdl)
        return f_path

    def authenticate(self, username, password):
        user = self._db_mgr.get_user_info(username)
        if not user:
            raise MEAuthException('User "%s" does not found'%username)

        pwd_hash = user[DBK_USER_PWD_HASH]
        if hashlib.sha1(password).hexdigest() != pwd_hash:
            raise MEAuthException('Password is invalid')

        session_id = uuid.uuid4().hex
        self._db_mgr.add_session(session_id, username)
        return session_id

    def logout(self, session_id):
        self._db_mgr.del_session(session_id)

    def create_user(self, session_id, username, password, roles):
        self._check_role(session_id, ROLE_CF)
        pwd_hash =  hashlib.sha1(password).hexdigest()
        self._db_mgr.create_user(username, pwd_hash, roles)

    def change_user_roles(self, session_id, username, roles):
        self._check_role(session_id, ROLE_CF)
        self._db_mgr.update_user_info(username, roles=roles)

    def change_user_password(self, session_id, new_password):
        user = self._db_mgr.get_user_by_session(session_id)
        if user is None:
            raise MEAuthException('Unknown user session!')

        pwd_hash =  hashlib.sha1(new_password).hexdigest()
        self._db_mgr.update_user_info(user[DBK_USERNAME], \
                pwd_hash=pwd_hash)

    def _check_role(self, session_id, role):
        user = self._db_mgr.get_user_by_session(session_id)
        if user is None:
            raise MEAuthException('Unknown user session!')

        roles = user[DBK_ROLES]
        if role not in roles:
            raise MEPermException('User does not have permissions for this action!')

    def get_cluster_config(self, session_id):
        self._check_role(session_id, ROLE_RO)
        return self._db_mgr.get_cluster_config()

    def configure_cluster(self, session_id, config):
        self._check_role(session_id, ROLE_CF)
        self._db_mgr.set_cluster_config(config)

    def show_nodes(self, session_id, filters={}, rows=None, pad=None):
        self._check_role(session_id, ROLE_RO)
        pass

    def start_nodes(self, session_id, nodes_list=[]):
        self._check_role(session_id, ROLE_SS)
        pass
    
    def reload_nodes(self, session_id, nodes_list=[]):
        self._check_role(session_id, ROLE_SS)
        pass

    def stop_nodes(self, session_id, nodes_list=[]):
        self._check_role(session_id, ROLE_SS)
        pass

    def upgrade_nodes(self, session_id):
        self._check_role(session_id, ROLE_UPGR)
        pass


