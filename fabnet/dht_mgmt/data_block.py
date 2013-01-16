#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.data_block

@author Konstantin Andrusenko
@date October 06, 2012
"""
from datetime import datetime
import time
import struct
import hashlib

DATA_BLOCK_LABEL = 'FDB01'
STRUCT_FMT = '<5sd20sb20s20s'


class DataBlockHeader:
    HEADER_LEN = struct.calcsize(STRUCT_FMT)

    @classmethod
    def pack(self, key, replica_count, checksum, user_id=None):
        if not user_id:
            user_id = 0
        if type(user_id) in (str, unicode):
            #FIXME: logger.warning('User ID should be integer value for saving into block metadata')
            user_id = 0
        t0 = datetime.utcnow()
        unixtime = time.mktime(t0.timetuple())

        try:
            header = struct.pack(STRUCT_FMT, DATA_BLOCK_LABEL, unixtime, key.decode('hex'), \
                            replica_count, checksum.decode('hex'), ('%040x'%user_id).decode('hex'))
        except Exception, err:
            raise Exception('Data block header packing failed! Details: %s'%err)

        return header

    @classmethod
    def unpack(cls, data):
        header = data[:cls.HEADER_LEN]
        try:
            db_label, put_unixtime, primary_key, replica_count, checksum, user_id = struct.unpack(STRUCT_FMT, header)
        except Exception, err:
            raise Exception('Data block header is invalid! Details: %s'%err)

        if db_label != DATA_BLOCK_LABEL:
            raise Exception('Corrupted data block! No block label found')

        return primary_key.encode('hex'), replica_count, checksum.encode('hex'), long(user_id.encode('hex'), 16), put_unixtime

    @classmethod
    def check_raw_data(cls, binary_data, exp_checksum=None):
        header = binary_data.read(cls.HEADER_LEN)

        _, _, checksum, user_id, _ = cls.unpack(header)

        if exp_checksum and exp_checksum != checksum:
            raise Exception('Data checksum is not equal to expected')

        h_func = hashlib.sha1('')
        while True:
            chunk = binary_data.get_next_chunk()
            if chunk is None:
                break
            h_func.update(chunk)

        if checksum != h_func.hexdigest():
            raise Exception('Data block has bad checksum')


