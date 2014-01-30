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

import paramiko

class check_auth:
    roles_map = {}
    def __init__(self, *roles):
        self.roles = roles

    def __call__(self_a, method):
        self_a.roles_map[method.__name__] = self_a.roles
        def decorated(self, session_id, *args, **kw_args):
            if not isinstance(self, ManagementEngineAPI):
                raise Exception('decorator @check_auth(<roles>) should '\
                        'be used for ManagementEngineAPI class methods')

            self._check_roles(session_id, self_a.roles)
            return method(self, session_id, *args, **kw_args)
        return decorated

class ManagementEngineAPI(object):
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

    def _check_roles(self, session_id, need_roles):
        user = self._db_mgr.get_user_by_session(session_id)
        if user is None:
            raise MEAuthException('Unknown user session!')

        roles = user[DBK_ROLES] 
        for role in roles:
            if role in need_roles:
                return
        raise MEPermException('User does not have permissions for this action!')

    def get_allowed_methods(self, session_id):
        user = self._db_mgr.get_user_by_session(session_id)
        if user is None:
            raise MEAuthException('Unknown user session!')

        roles = user[DBK_ROLES] 
        methods = []
        for item in dir(self):
            item_roles = check_auth.roles_map.get(item, None)
            if not item_roles:
                continue
            for role in roles:
                if role in item_roles:
                    methods.append(item)
        return methods

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

    @check_auth(ROLE_UM)
    def get_available_roles(self, session_id):
        return ROLES_DESC

    @check_auth(ROLE_UM)
    def create_user(self, session_id, username, password, roles):
        if len(password) < 3:
            raise MEInvalidArgException('Password is too short!')

        pwd_hash =  hashlib.sha1(password).hexdigest()
        self._db_mgr.create_user(username, pwd_hash, roles)

    @check_auth(ROLE_UM)
    def get_user_info(self, session_id, username):
        user = self._db_mgr.get_user_info(username)
        if not user:
            return user
        session = self._db_mgr.get_user_last_session(username)
        user[DBK_LAST_SESSION] = session
        return user

    @check_auth(ROLE_UM)
    def remove_user(self, session_id, username):
        user = self._db_mgr.get_user_info(username)
        if not user:
            raise MENotFoundException('User "%s" does not found!'%username)
        self._db_mgr.remove_user(username)

    @check_auth(ROLE_UM)
    def change_user_roles(self, session_id, username, roles):
        self._db_mgr.update_user_info(username, roles=roles)

    def change_user_password(self, session_id, username, new_password):
        if username:
            self._check_roles(session_id, ROLE_UM)
            user = self._db_mgr.get_user_info(username)
            if not user:
                raise MENotFoundException('User "%s" does not found!'%username)
        else:
            user = self._db_mgr.get_user_by_session(session_id)
            if user is None:
                raise MEAuthException('Unknown user session!')

        if len(new_password) < 3:
            raise MEInvalidArgException('Password is too short!')

        pwd_hash =  hashlib.sha1(new_password).hexdigest()
        self._db_mgr.update_user_info(user[DBK_USERNAME], \
                pwd_hash=pwd_hash)

    @check_auth(ROLE_RO)
    def get_cluster_config(self, session_id):
        return self._db_mgr.get_cluster_config()

    @check_auth(ROLE_CF)
    def configure_cluster(self, session_id, config):
        self._db_mgr.set_cluster_config(config)

    @check_auth(ROLE_NM)
    def install_new_node(self, session_id, ssh_address, ssh_user, ssh_pwd, node_name, \
            node_type, node_address):
        client = paramiko.SSHClient()
        client.get_host_keys().add(ssh_address, 'ssh-rsa', paramiko.RSAKey.generate(1024))
        client.connect(ssh_address, username=ssh_user, password=ssh_pwd)
        stdin, stdout, stderr = client.exec_command('ls')

        #sftp = s.open_sftp()
        #sftp.put('/home/me/file.ext', '/remote/home/file.ext')
        self._db_mgr.append_node(node_name, node_type, node_address)

    @check_auth(ROLE_SS)
    def show_nodes(self, session_id, filters={}, rows=None):
        pass

    @check_auth(ROLE_SS)
    def start_nodes(self, session_id, nodes_list=[]):
        pass

    @check_auth(ROLE_SS)
    def reload_nodes(self, session_id, nodes_list=[]):
        pass

    @check_auth(ROLE_SS)
    def stop_nodes(self, session_id, nodes_list=[]):
        pass

    @check_auth(ROLE_SS)
    def upgrade_nodes(self, session_id):
        pass



