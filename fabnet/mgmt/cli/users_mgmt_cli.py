#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.mgmt.management_agent
@author Konstantin Andrusenko
@date July 28, 2013

This module contains the implementation of UsersMgmtCLIHandler class
"""
from fabnet.mgmt.cli.base_cli import *

class UsersMgmtCLIHandler(BaseMgmtCLIHandler):
    @validator(str, (str, 1))
    @cli_command('create-user', 'create_user', 'createuser', 'cru')
    def command_create_user(self, params):
        '''<user name> <role1> [<role2> ...]
        Create new user account
        This command creates new user account in management database
        Use show-roles command for display all possible roles list
        '''
        pwd = self.readline(prompt='Enter new user password: ', echo=False)
        self.writeline('')
        re_pwd = self.readline(prompt='Verify new user password: ', echo=False)
        self.writeline('')
        if pwd != re_pwd:
            self.writeresponse('Error: password verification failed!')
            return

        self.mgmtManagementAPI.create_user(self.session_id, \
                params[0], pwd, params[1:])
        self.writeresponse('user "%s" is created!'%params[0])

    @validator()
    @cli_command('show-roles', 'get_available_roles', 'shroles', 'shr')
    def command_show_roles(self, params):
        '''
        Show available roles
        '''
        roles = self.mgmtManagementAPI.get_available_roles(self.session_id)
        for role, role_desc in roles.items():
            self.writeline('%s - %s'%(role.ljust(12), role_desc))

    @validator(str)
    @cli_command('user-info', 'get_user_info', 'shuser', 'userinfo')
    def command_show_user(self, params):
        '''<user name>
        Show user account information
        '''
        user_info = self.mgmtManagementAPI.get_user_info(self.session_id, params[0])
        if not user_info:
            self.writeline('Error! No user "%s" found!'%params[0])
            return
        self.writeline('User name: %s'%user_info[DBK_USERNAME])
        self.writeline('User roles: %s'%', '.join(user_info[DBK_ROLES]))
        last_session = user_info[DBK_LAST_SESSION]
        if last_session:
            self.writeline('Last session start: %s'%last_session[DBK_START_DT])
        else:
            self.writeline('No sessions found')

    @validator(str, (str, 1))
    @cli_command('change-user-roles', 'change_user_roles', 'churoles')
    def command_change_user_roles(self, params):
        '''<user name> <role1> [<role2> ...]
        Change user roles
        Use show-roles command for display all possible roles list
        '''
        self.mgmtManagementAPI.change_user_roles(self.session_id, \
                params[0], params[1:])
        self.writeline('User roles are installed for user "%s"'%params[0])

    @validator(str)
    @cli_command('remove-user', 'remove_user', 'rmuser')
    def command_remove_user(self, params):
        '''<user name>
        Remove user account
        '''
        self.mgmtManagementAPI.remove_user(self.session_id, params[0])
        self.writeline('User "%s" is removed.'%params[0])

    @validator()
    @cli_command('change-pwd')
    def command_remove_user(self, params):
        '''[<user name>]
        Change user password
        If user name does not specified, current user should be used 
        '''
        username = None
        if len(params) > 0:
            username = params[0]

        pwd = self.readline(prompt='Enter new user password: ', echo=False)
        self.writeline('')
        re_pwd = self.readline(prompt='Verify new user password: ', echo=False)
        self.writeline('')
        if pwd != re_pwd:
            self.writeresponse('Error: password verification failed!')
            return

        self.mgmtManagementAPI.change_user_password(self.session_id, username, pwd)
        self.writeresponse('Password is changed!')

