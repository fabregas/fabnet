
from telnetsrv.threaded import TelnetHandler, command

class InvalidArgumentsException(Exception):
    pass

class cli_command:
    cli_commands = {}
    def __init__(self, cmd_name, api_method_name=None, *aliases):
        self.cmd_name = cmd_name
        self.api_method_name = api_method_name
        self.aliases = aliases

    def __call__(self, method):
        self.cli_commands[self.cmd_name] = (method.__name__, self.api_method_name, \
                self.aliases, method.__doc__)
        return method

class validator:
    def __init__(self, *val_expr_list):
        self.val_expr_list = val_expr_list

    def __validate_args(self, params):
        def validate(val, v_type):
            if not isinstance(val, v_type):
                raise InvalidArgumentsException(val)
        i = 0
        for validator in self.val_expr_list:
            if type(validator) == tuple:
                p_c_type = validator[0]
                min_cnt = validator[1]

                while True:
                    if min_cnt > 0 and len(params) < i+1:
                        raise InvalidArgumentsException()
                    if len(params) < i+1:
                        break
                    validate(params[i], p_c_type)
                    i += 1
                    min_cnt -= 1
            else:
                if len(params) < i+1:
                    raise InvalidArgumentsException()
                validate(params[i], validator)
                i += 1


    def __call__(self, method):
        def validate_and_call(self_m, params, *others):
            try:
                try:
                    self.__validate_args(params) 
                except InvalidArgumentsException, err:
                    for cmd, (m_name, _, _, help_msg) in cli_command.cli_commands.items():
                        if m_name != method.__name__:
                            continue
                        doc = help_msg.split("\n")
                        docp = doc[0].strip()
                        self_m.writeresponse("Usage: %s"%docp)
                        return

                    self_m.writeresponse('Unknown method %s'%method.__name__)
                    return

                return method(self_m, params, *others)
            except Exception, err:
                self_m.writeresponse('Unexpected error: %s\n'%err)

        return validate_and_call


class MgmtCLIHandler(TelnetHandler):
    PROMPT = 'mgmt-cli> '
    WELCOME = '%s\n%s\n%s'%('='*80, '  Welcome to fabnet management console  '.center(80, '='), '='*80)

    authNeedUser = True
    authNeedPass = True

    mgmtManagementAPI = None

    def authCallback(self, username, password):
        if not self.mgmtManagementAPI:
            return

        try:
            self.session_id = self.mgmtManagementAPI.authenticate(username, password)
        except Exception, err:
            self.writeresponse('ERROR! %s'%err)
            raise err

    def session_start(self):
        if not self.mgmtManagementAPI.is_initialized():
            self.writeline('!!!Management engine does not initialized!!!')
            pwd = self.readline(prompt='Key storage password: ', echo=False)
            self.mgmtManagementAPI.initialize(pwd)

        self.COMMANDS = {}
        allowed_methods = self.mgmtManagementAPI.get_allowed_methods(self.session_id)
        for name, (method, api_method, aliases, _) in cli_command.cli_commands.items():
            if api_method and api_method not in allowed_methods:
                continue
            name = name.upper()
            self.COMMANDS[name] = getattr(self, method)
            for alias in aliases:
                self.COMMANDS[alias.upper()] = self.COMMANDS[name]

    def session_end(self):
        self.mgmtManagementAPI.logout(self.session_id)

    @cli_command('exit')
    def command_exit(self, params):
        """
        Exit the command shell
        """
        self.RUNSHELL = False
        self.writeline("Goodbye")

    @cli_command('help')
    def command_help(self, params):
        """[<command>]
        Display help
        Display either brief help on all commands, or detailed
        help on a single command passed as a parameter.
        """
        if params:
            cmd = params[0].upper()

            if self.COMMANDS.has_key(cmd):
                method = cli_command.cli_commands.get(cmd.lower(), None)
                _, _, aliases, help_msg = method
                doc = help_msg.split("\n")
                docp = doc[0].strip()
                docl = '\n'.join( [l.strip() for l in doc[2:]] )
                if not docl.strip():  # If there isn't anything here, use line 1
                    docl = doc[1].strip()
                self.writeline("%s %s\n\n%s" % (cmd, docp, docl))
                return
            else:
                self.writeline("Command '%s' not known" % cmd)
                self.writeline("")

        self.writeline("Help on built in commands\n")

        keys = self.COMMANDS.keys()
        keys.sort()
        for cmd in keys:
            _, _, aliases, help_msg = cli_command.cli_commands[cmd.lower()]
            #if getattr(method, 'hidden', False):
            #    continue
            doc = help_msg.split("\n")
            docp = doc[0].strip()
            docs = doc[1].strip()
            if len(docp) > 0:
                docps = "%s - %s" % (docp, docs, )
            else:
                docps = "- %s" % (docs, )
            self.writeline("%s %s" % (cmd, docps))

    @validator(str, (str, 1))
    @cli_command('create-user', 'create_user')
    def command_create_user(self, params):
        '''<user name> <role1> [<role2>, ...]
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
    @cli_command('show-roles', 'get_available_roles')
    def command_show_roles(self, params):
        '''
        Show available roles
        '''
        roles = self.mgmtManagementAPI.get_available_roles(self.session_id)
        for role, role_desc in roles.items():
            self.writeline('%s - %s'%(role.ljust(12), role_desc))

    @validator(str)
    @cli_command('user-info', 'get_user_info')
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

import SocketServer
class TelnetServer(SocketServer.TCPServer):
    allow_reuse_address = True

from fabnet.mgmt.mgmt_db import MgmtDatabaseManager
from fabnet.mgmt.management_engine_api import ManagementEngineAPI
from fabnet.mgmt.constants import *

dbm = MgmtDatabaseManager('localhost')
mgmt_api = ManagementEngineAPI(dbm)
MgmtCLIHandler.mgmtManagementAPI = mgmt_api

server = TelnetServer(("0.0.0.0", 8023), MgmtCLIHandler)
server.serve_forever()
