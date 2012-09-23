#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.operations.get_data_range_request

@author Konstantin Andrusenko
@date September 25, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.dht_mgmt.constants import DS_NORMALWORK
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import logger

class GetRangeDataRequestOperation(OperationBase):
    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        dht_range = self.operator.get_dht_range()

        subranges = dht_range.get_subranges()
        if not subranges:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Range is not splitted!')

        ret_range, new_range = subranges

        try:
            node_address = packet.sender
            logger.debug('Starting subrange data transfering to %s'% node_address)
            for key, data in ret_range.iter_range():
                params = {'key': key, 'data': data}
                resp = self._init_operation(node_address, 'PutDataBlock', params, sync=True)
                if resp.ret_code:
                    raise Exception('Init PutDataBlock operation on %s error. Details: %s'%(node_address, resp.ret_message))

            self.operator.update_dht_range(new_range)
        except Exception, err:
            logger.error('GetRangeDataRequestOperation error: %s'%err)
            dht_range.join_subranges()
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Send range data failed: %s'%err)

        ret_range.move_to_trash()
        append_lst = [(ret_range.get_start(), ret_range.get_end(), node_address)]
        append_lst.append((new_range.get_start(), new_range.get_end(), self.operator.self_address))
        rm_lst = [(dht_range.get_start(), dht_range.get_end(), self.operator.self_address)]
        self._init_network_operation('UpdateHashRangeTable', {'append': append_lst, 'remove': rm_lst})

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
        if packet.ret_code != RC_OK:
            logger.info('Trying select other hash range...')
            self.operator.start_as_dht_member()
        else:
            logger.info('Changing node status to NORMALWORK')
            self.operator.status = DS_NORMALWORK
