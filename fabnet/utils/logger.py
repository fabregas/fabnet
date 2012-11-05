#!/usr/bin/python
"""
Copyright (C) 2011 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.logger
@author Konstantin Andrusenko
@date August 20, 2012

This module contains the fabnet logger initialization
"""
import logging, logging.handlers
import sys

def init_logger():
    logger = logging.getLogger('localhost')

    logger.setLevel(logging.INFO)

    if sys.platform == 'darwin':
        log_path = '/var/run/syslog'
    else:
        log_path = '/dev/log'

    hdlr = logging.handlers.SysLogHandler(address=log_path,
              facility=logging.handlers.SysLogHandler.LOG_DAEMON)
    #formatter = logging.Formatter('%(filename)s: %(levelname)s: %(message)s')

    formatter = logging.Formatter('FABNET %(levelname)s [%(threadName)s] %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger

logger = init_logger()
