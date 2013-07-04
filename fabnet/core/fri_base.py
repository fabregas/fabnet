#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.fri_base
@author Konstantin Andrusenko
@date August 20, 2012

This module contains the implementation of FabnetPacketRequest, FabnetPacketResponse classes.
"""
import os
import uuid
import struct
import zlib
import json
import tempfile

from constants import RC_OK, FRI_PROTOCOL_IDENTIFIER, FRI_PACKET_INFO_LEN, DEFAULT_CHUNK_SIZE


class FriException(Exception):
    pass


class FriBinaryData:
    def chunks_count(self):
        raise RuntimeError('Not implemented')

    def get_next_chunk(self):
        """Return next data chunk. None should be returned if EOF"""
        raise RuntimeError('Not implemented')

    def data(self):
        """Return all binary data in one chunk"""
        data = ''
        while True:
            chunk = self.get_next_chunk()
            if not chunk:
                break
            data += chunk
        return data

    def close(self):
        pass

class RamBasedBinaryData(FriBinaryData):
    def __init__(self, data, chunk_size=None):
        if not chunk_size:
            chunk_size = len(data)
        if chunk_size < 1:
            chunk_size = 1
        self.__chunk_size = chunk_size
        self.__data = data
        self.__last_idx = 0
        self.__chunks_count = self.chunks_count()

    def chunks_count(self):
        f_size = len(self.__data)
        cnt = f_size / self.__chunk_size
        if f_size % self.__chunk_size != 0:
            cnt += 1
        return cnt

    def get_next_chunk(self):
        if self.__last_idx >= self.__chunks_count:
            return None

        start = self.__chunk_size * self.__last_idx
        self.__last_idx += 1
        end = self.__chunk_size * self.__last_idx

        return self.__data[start:end]

    def data(self):
        return self.__data

class BinaryDataPointer:
    def __init__(self, file_path, remove_on_close=False):
        self.file_path = file_path 
        self.remove_on_close = remove_on_close

class FileBasedChunks(FriBinaryData):
    @classmethod
    def from_data_pointer(cls, bin_data_pointer): 
        return FileBasedChunks(bin_data_pointer.file_path, \
                remove_on_close=bin_data_pointer.remove_on_close)

    def __init__(self, file_path, chunk_size=DEFAULT_CHUNK_SIZE, remove_on_close=False):
        self.__file_path = file_path
        self.__chunk_size = chunk_size
        self.__f_obj = None
        self.__no_data_flag = False
        self.__read_bytes = 0
        self.__remove_on_close = remove_on_close

    def __del__(self):
        if self.__remove_on_close and os.path.exists(self.__file_path):
            os.remove(self.__file_path)

    def chunks_count(self):
        f_size = os.path.getsize(self.__file_path) - self.__read_bytes
        cnt = f_size / self.__chunk_size
        if f_size % self.__chunk_size != 0:
            cnt += 1
        return cnt

    def read(self, block_size):
        if self.__no_data_flag:
            return None

        try:
            if not self.__f_obj:
                self.__f_obj = open(self.__file_path, 'rb')

            chunk = self.__f_obj.read(block_size)
            if not chunk:
                self.close()
                return None
            self.__read_bytes += len(chunk)
            return chunk
        except IOError, err:
            self.close()
            raise FSHashRangesException('Cant read data from file system. Details: %s'%err)
        except Exception, err:
            self.close()
            raise err

    def get_next_chunk(self):
        return self.read(self.__chunk_size)

    def close(self):
        self.__no_data_flag = True
        if self.__f_obj:
            self.__f_obj.close()

def serialize_binary_data(binary_data):
    if isinstance(binary_data, BinaryDataPointer):
        return binary_data

    if not isinstance(binary_data, FriBinaryData): 
        raise FriException('Invalid binary data type: %s'%type(binary_data))

    fd, tmp_path = tempfile.mkstemp('-serialized_binary')    
    while True:
        chunk = binary_data.get_next_chunk()
        if chunk is None:
            break
        os.write(fd, chunk)
    binary_data.close()
    os.close(fd)
    return BinaryDataPointer(tmp_path, remove_on_close=True)


class FriBinaryProcessor:
    NEED_COMPRESSION = False

    @classmethod
    def get_expected_len(cls, data):
        p_info = data[:FRI_PACKET_INFO_LEN]
        if len(p_info) != FRI_PACKET_INFO_LEN:
            raise FriException('Invalid FRI packet! No packet header found')

        try:
            prot, packet_len, header_len = struct.unpack('<4sqq', p_info)
        except Exception, err:
            raise FriException('Invalid FRI packet! Packet information is corrupted: %s'%err)

        if prot != FRI_PROTOCOL_IDENTIFIER:
            raise FriException('Invalid FRI packet! Protocol is mismatch')

        return packet_len, header_len


    @classmethod
    def from_binary(cls, data, packet_len, header_len):
        if len(data) != int(packet_len):
            raise FriException('Invalid FRI packet! Packet length %s is differ to expected %s'%(len(data), packet_len))

        header = data[FRI_PACKET_INFO_LEN:FRI_PACKET_INFO_LEN+header_len]
        if len(header) != int(header_len):
            raise FriException('Invalid FRI packet! Header length %s is differ to expected %s'%(len(header), header_len))

        try:
            json_header = json.loads(header)
        except Exception, err:
            raise FriException('Invalid FRI packet! Header is corrupted: %s'%err)

        bin_data = data[header_len+FRI_PACKET_INFO_LEN:]
        if bin_data and cls.NEED_COMPRESSION:
            bin_data = zlib.decompress(bin_data)

        return json_header, bin_data

    @classmethod
    def to_binary(cls, header_obj, bin_data=''):
        try:
            header = json.dumps(header_obj)
        except Exception, err:
            raise FriException('Cant form FRI packet! Header "%s" is corrupted: %s'%(header_obj, err))

        if bin_data and cls.NEED_COMPRESSION:
            bin_data = zlib.compress(bin_data)

        h_len = len(header)
        packet_data = header + bin_data
        p_len = len(packet_data) + FRI_PACKET_INFO_LEN
        p_info = struct.pack('<4sqq', FRI_PROTOCOL_IDENTIFIER, p_len, h_len)

        return p_info + packet_data


class FabnetPacket:
    is_request = False
    is_response = False

    @classmethod
    def create(cls, raw_packet):
        if type(raw_packet) != dict:
            raise FriException('Cant create FabnetPacket. '
                        'Expected raw packet with dict type, but "%s" occured'%type(raw_packet))

        if raw_packet.has_key('method'):
            return FabnetPacketRequest(**raw_packet)
        else:
            return FabnetPacketResponse(**raw_packet)

    def __init__(self, **packet):
        self.message_id = packet.get('message_id', None)
        self.session_id = packet.get('session_id', None)

        self.binary_data = packet.get('binary_data', None)
        if type(self.binary_data) == str:
            self.binary_data = RamBasedBinaryData(self.binary_data)
        self.binary_chunk_idx = packet.get('binary_chunk_idx', 0)
        self.binary_chunk_cnt = packet.get('binary_chunk_cnt', 0)

    def __del__(self):
        if isinstance(self.binary_data, FriBinaryData):
            self.binary_data.close()

    def validate(self):
        """This method may be implemented
           in inherited class for packet validation
        """
        pass

    def dump(self, with_bin=True):
        header_json = self.to_dict()
        if self.binary_data and with_bin:
            binary_data = self.binary_data.data()
        else:
            binary_data = ''
        data = FriBinaryProcessor.to_binary(header_json, binary_data)
        return data

    def dump_next_chunk(self):
        header_json = self.to_dict()
        binary_data = ''
        if self.binary_data:
            binary_data = self.binary_data.get_next_chunk()
        if not binary_data:
            return None
        data = FriBinaryProcessor.to_binary(header_json, binary_data)
        return data

    def to_dict(self):
        """This method may be extended in inherited class"""
        ret_dict = {'message_id': self.message_id}

        if self.session_id:
            ret_dict['session_id'] = self.session_id
        if self.binary_chunk_idx:
            ret_dict['binary_chunk_idx'] = self.binary_chunk_idx
        if self.binary_chunk_cnt:
            ret_dict['binary_chunk_cnt'] = self.binary_chunk_cnt

        return ret_dict

    def __str__(self):
        return str(self.__repr__())


class FabnetPacketRequest(FabnetPacket):
    is_request = True
    is_response = False
    def __init__(self, **packet):
        FabnetPacket.__init__(self, **packet)

        self.is_multicast = packet.get('is_multicast', None)
        self.sync = packet.get('sync', False)
        if not self.message_id:
            self.message_id = str(uuid.uuid1())
        self.method = packet.get('method', None)
        self.sender = packet.get('sender', None)
        self.parameters = packet.get('parameters', {})
        self.role = None

        self.validate()

    def copy(self):
        return FabnetPacketRequest(**self.to_dict())

    def validate(self):
        if self.message_id is None:
            raise FriException('Invalid packet: message_id does not exists')

        if self.method is None:
            raise FriException('Invalid packet: method does not exists')


    def to_dict(self):
        ret_dict = FabnetPacket.to_dict(self)
        ret_dict.update({'method': self.method, \
                'sender': self.sender, \
                'sync': self.sync})

        if self.parameters:
            ret_dict['parameters'] = self.parameters
        if self.is_multicast:
            ret_dict['is_multicast'] = self.is_multicast

        return ret_dict

    def __repr__(self):
        sync_s = 'sync' if self.sync else 'async'
        cast_s = 'multicast' if self.is_multicast else 'unicast'
        h_bin = '[HAS_BIN]' if self.binary_data else ''
        return '{%s}[%s][%s][%s]%s %s %s'%(self.message_id, self.sender, \
                    sync_s, cast_s, h_bin, self.method, str(self.parameters))


class FabnetPacketResponse(FabnetPacket):
    is_request = False
    is_response = True
    def __init__(self, **packet):
        FabnetPacket.__init__(self, **packet)

        self.ret_code = packet.get('ret_code', RC_OK)
        self.ret_message = str(packet.get('ret_message', ''))
        self.ret_parameters = packet.get('ret_parameters', {})
        self.from_node = packet.get('from_node', None)

    def to_dict(self):
        ret_dict = FabnetPacket.to_dict(self)
        ret_dict.update({'ret_code': self.ret_code,
                'ret_message': self.ret_message})

        if self.ret_parameters:
            ret_dict['ret_parameters'] = self.ret_parameters
        if self.from_node:
            ret_dict['from_node'] = self.from_node

        return ret_dict


    def __str__(self):
        return str(self.__repr__())

    def __repr__(self):
        h_bin = '[HAS_BIN]' if self.binary_data else ''
        return '{%s}[%s]%s %s %s %s'%(self.message_id, self.from_node, h_bin,
                    self.ret_code, self.ret_message, str(self.ret_parameters)[:100])




