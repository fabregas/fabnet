#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.update_hash_range_table

@author Konstantin Andrusenko
@date September 26, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse

class UpdateHashRangeTableOperation(OperationBase):
    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        return packet

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """

        append_lst = packet.parameters.get('append', [])
        rm_lst = packet.parameters.get('remove', [])

        try:
            for rm_range in rm_lst:
               self.operator.ranges_table.remove(rm_range[0])

            for app_range in append_lst:
                self.operator.ranges_table.append(app_range[0], app_range[1], app_range[2])
        except Exception, err:
            logger.error('UpdateHashRangeTable error: %s'%err)
            #FIXME: hash range in this point may be corrupted... restore it from neighbour
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err))

        return FabnetPacketResponse()


    def callback(self, packet, sender=None):
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
