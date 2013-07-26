#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.mgmt.constants
@author Konstantin Andrusenko
@date July 24, 2013
"""

#database keys
DBK_CLUSTER_CONFIG = 'cluster_config'
DBK_USERS = 'users'
DBK_SESSIONS = 'sessions'

DBK_ID = '_id'
DBK_CONFIG_KS = 'key_storage' 
DBK_CONFIG_CLNAME = 'cluster_name'
DBK_USERNAME = 'username'
DBK_START_DT = 'start_dt'
DBK_ROLES = 'roles'
DBK_USER_PWD_HASH = 'password_hash'
DBK_LAST_SESSION = 'last_session'

#user roles
ROLE_RO = 'readonly'
ROLE_UM = 'usersmanage'
ROLE_CF = 'configure'
ROLE_SS = 'startstop'
ROLE_UPGR = 'upgrade'

ROLES_DESC = {ROLE_RO: 'Read only access',
            ROLE_UM: 'Manage users accounts access',
            ROLE_CF: 'Configure cluster access',
            ROLE_SS: 'Start/Stop/Reload nodes access',
            ROLE_UPGR: 'Upgrade nodes access'}

