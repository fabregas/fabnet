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

DATA_BLOCK_LABEL = 'FDB'
STRUCT_FMT = '<3sd40sb40s'


class DataBlockHeader:
    HEADER_LEN = struct.calcsize(STRUCT_FMT)

    @classmethod
    def pack(self, key, replica_count, checksum):
        t0 = datetime.utcnow()
        unixtime = time.mktime(t0.timetuple())

        try:
            header = struct.pack(STRUCT_FMT, DATA_BLOCK_LABEL, unixtime, str(key), \
                            replica_count, str(checksum))
        except Exception, err:
            raise Exception('Data block header packing failed! Details: %s'%err)

        return header

    @classmethod
    def unpack(cls, data):
        header = data[:cls.HEADER_LEN]
        try:
            db_label, put_unixtime, primary_key, replica_count, checksum = struct.unpack(STRUCT_FMT, header)
        except Exception, err:
            raise Exception('Data block header is invalid! Details: %s'%err)

        if db_label != DATA_BLOCK_LABEL:
            raise Exception('Corrupted data block! No block label found')

        return primary_key, replica_count, checksum, put_unixtime

    @classmethod
    def check_raw_data(cls, binary_data, exp_checksum=None):
        header = binary_data.read(cls.HEADER_LEN)

        _, _, checksum, _ = cls.unpack(header)

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


