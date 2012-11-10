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

class DataBlock:
    header_len = struct.calcsize(STRUCT_FMT)

    def __init__(self, raw_data='', raw_checksum=None):
        self.raw_data = raw_data
        self.raw_checksum = raw_checksum
        self.__packed = False

    def validate(self):
        checksum = hashlib.sha1(self.raw_data).hexdigest()
        if checksum != self.raw_checksum:
            raise Exception('Data block is corrupted!')

    def pack(self, key, replica_count):
        if self.__packed:
            return self.raw_data, self.raw_checksum

        t0 = datetime.utcnow()
        unixtime = time.mktime(t0.timetuple())

        try:
            header = struct.pack(STRUCT_FMT, DATA_BLOCK_LABEL, unixtime, str(key), \
                            replica_count, str(self.raw_checksum))
        except Exception, err:
            raise Exception('Data block header packing failed! Details: %s'%err)

        self.raw_data = header + self.raw_data
        self.raw_checksum = hashlib.sha1(self.raw_data).hexdigest()
        self.__packed = True

        return self.raw_data, self.raw_checksum

    @classmethod
    def check_raw_data(cls, packet_data, checksum):
        raw_data = packet_data[cls.header_len:]
        if checksum != hashlib.sha1(raw_data).hexdigest():
            raise Exception('Data has bad checksum')


    @classmethod
    def read_header(cls, data):
        header = data[:cls.header_len]
        try:
            db_label, put_unixtime, primary_key, replica_count, checksum = struct.unpack(STRUCT_FMT, header)
        except Exception, err:
            raise Exception('Data block header is invalid! Details: %s'%err)

        if db_label != DATA_BLOCK_LABEL:
            raise Exception('Corrupted data block! No block label found')

        #datetime.fromtimestamp(put_unixtime)
        return primary_key, replica_count, checksum, put_unixtime

    def unpack(self):
        primary_key, replica_count, checksum, put_dt = self.read_header(self.raw_data)

        self.raw_data = self.raw_data[self.header_len:]
        self.raw_checksum = checksum
        self.__packed = False
        self.validate()

        return self.raw_data, self.raw_checksum

