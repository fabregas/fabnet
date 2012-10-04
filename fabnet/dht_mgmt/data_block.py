#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.data_block

@author Konstantin Andrusenko
@date October 06, 2012
"""
import struct
import hashlib

DATA_BLOCK_LABEL = 'FDB'

class DataBlock:
    header_len = struct.calcsize('<3s40sb40s')

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

        try:
            header = struct.pack('<3s40sb40s', DATA_BLOCK_LABEL, str(key), \
                            replica_count, str(self.raw_checksum))
        except Exception, err:
            raise Exception('Data block header packig failed! Details: %s'%err)

        self.raw_data = header + self.raw_data
        self.raw_checksum = hashlib.sha1(self.raw_data).hexdigest()
        self.__packed = True

        return self.raw_data, self.raw_checksum

    @classmethod
    def read_header(cls, data):
        header = data[:cls.header_len]
        try:
            db_label, primary_key, replica_count, checksum = struct.unpack('<3s40sb40s', header)
        except Exception, err:
            raise Exception('Data block header is invalid! Details: %s'%err)

        if db_label != DATA_BLOCK_LABEL:
            raise Exception('Corrupted data block! No block label found')

        return primary_key, replica_count, checksum

    def unpack(self):
        primary_key, replica_count, checksum = self.read_header(self.raw_data)

        self.raw_data = self.raw_data[self.header_len:]
        self.raw_checksum = checksum
        self.__packed = False
        self.validate()

        return self.raw_data, self.raw_checksum

