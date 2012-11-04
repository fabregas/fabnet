#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.operations.upgrade_node_operation

@author Konstantin Andrusenko
@date Novenber 5, 2012
"""
import os
from datetime import datetime

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import ET_ALERT

GIT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

class UpgradeNodeOperation(OperationBase):
    ROLES = [NODE_ROLE]

    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        return packet

    def __upgrade_node(self, origin_url):
        old_curdir = os.path.abspath(os.curdir())
        try:
            if not origin_url:
                raise Exception('origin_url does not found')

            os.chdir(GIT_HOME)
            os.system('git config --local --replace-all remote.origin.url %s'%origin_url)
            ret = os.system('git pull')
            if ret:
                raise Exception('git pull failed')

            ret = os.system('./fabnet/bin/upgrade-node')
            if ret:
                raise Exception('upgrade-node script failed!')
        except Exception, err:
            self._throw_event(ET_ALERT, 'UpgradeNodeOperation failed: %s'%err)
            logger.error('[UpgradeNodeOperation] %s'%err)
        finally:
            os.chdir(old_curdir)


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        self.__upgrade_node(packet.parameters.get('origin_repo_url', None))


    def callback(self, packet, sender):
        """In this method should be implemented logic of processing
        response packet from requested node

        @param packet - object of FabnetPacketResponse class
        @param sender - address of sender node.
        If sender == None then current node is operation initiator

        @return object of FabnetPacketResponse
                that should be resended to current node requestor
                or None for disabling packet resending
        """
        pass