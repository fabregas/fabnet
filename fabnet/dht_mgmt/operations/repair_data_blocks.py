#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.repair_data_blocks

@author Konstantin Andrusenko
@date November 10, 2012
"""
import traceback
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.utils.logger import logger
from fabnet.core.constants import NODE_ROLE, ET_INFO, ET_ALERT


class RepairDataBlocksOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'RepairDataBlocks'


    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        self._lock()
        try:
            stat = self.operator.repair_data_process(packet.parameters)
            self._throw_event(ET_INFO, 'RepairDataBlocks', stat)
        except Exception, err:
            self._throw_event(ET_ALERT, 'RepairDataBlocks', err)
            logger.write = logger.debug
            traceback.print_exc(file=logger)
        finally:
            self._unlock()

        return packet


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        pass

