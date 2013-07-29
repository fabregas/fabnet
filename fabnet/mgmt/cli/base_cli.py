#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.mgmt.management_agent
@author Konstantin Andrusenko
@date July 27, 2013

This module contains the implementation of BaseMgmtCLIHandler class
and decorators for easy CLI commands implement
"""

from fabnet.mgmt.cli.telnetserver.threaded import TelnetHandler

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


class BaseMgmtCLIHandler(TelnetHandler):
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
        if getattr(self, 'session_id', None) is None:
            return
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

            method = cli_command.cli_commands.get(cmd.lower(), None)
            if method:
                _, _, aliases, help_msg = method
                doc = help_msg.split("\n")
                docp = doc[0].strip()
                docl = '\n'.join( [l.strip() for l in doc[2:]] )
                if not docl.strip():  # If there isn't anything here, use line 1
                    docl = doc[1].strip()
                if aliases:
                    al_doc = '\nCommand aliases: %s'%', '.join(aliases)
                else:
                    al_doc = ''
                self.writeline("%s %s\n\n%s%s" % (cmd, docp, docl, al_doc))
                return
            else:
                self.writeline("Command '%s' not known" % cmd)
                self.writeline("")

        self.writeline("Help on built in commands\n")

        keys = self.COMMANDS.keys()
        keys.sort()
        for cmd in keys:
            method = cli_command.cli_commands.get(cmd.lower(), None)
            if not method:
                continue
            _, _, aliases, help_msg = method

            doc = help_msg.split("\n")
            docp = doc[0].strip()
            docs = doc[1].strip()
            if len(docp) > 0:
                docps = "%s - %s" % (docp, docs, )
            else:
                docps = "- %s" % (docs, )
            self.writeline("%s %s" % (cmd, docps))




