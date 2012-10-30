#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.sessions_manager
@author Konstantin Andrusenko
@date August 30, 2012

This module contains the implementation of SessionsManager class.
"""
from datetime import datetime

class Session:
    def __init__(self, session_id, role):
        self.session_id = session_id
        self.role = role
        self.start_dt = datetime.now()

class SessionsManager:
    #TODO: make me smarter for flushing sessions...
    def __init__(self, homedir):
        self.homedir = homedir
        self.__sessions = {}

    def get(self, session_id):
        return self.__sessions.get(session_id, None)

    def append(self, session_id, role):
        self.__sessions[session_id] = Session(session_id, role)
