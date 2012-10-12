#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package client.nibbler
@author Konstantin Andrusenko
@date October 12, 2012

This module contains the implementation of user API to idepositbox service.
"""
import os
import tempfile
import hashlib
from client.constants import FILE_ITER_BLOCK_SIZE, CHUNK_SIZE
from fabnet_gateway import FabnetGateway
from metadata import *


class FileIterator:
    def __init__(self, file_path, is_tmp=True):
        self.file_path = file_path
        self.is_tmp = is_tmp

    def __iter__(self):
        if not os.path.exists(self.file_path):
            raise Exception('File %s does not exists!'%self.file_path)

        f_obj = open(self.file_path, 'rb')
        try:
            while True:
                data = f_obj.read(FILE_ITER_BLOCK_SIZE)
                if len(data) == 0:
                    return

                yield data
        except Exception, err:
            raise Exception('Reading file %s failed: %s'%(self.file_path, err))
        finally:
            f_obj.close()
            if self.is_tmp:
                os.remove(self.file_path)



class Nibbler:
    def __init__(self, fabnet_host, security_provider):
        self.security_provider = security_provider
        self.fabnet_gateway = FabnetGateway(fabnet_host, security_provider)
        self.metadata = None


    def __get_file(self, file_obj):
        f_obj, tmpfl = tempfile.mkstemp(prefix='nibbler')
        try:
            for chunk in file_obj.chunks:
                data = self.fabnet_gateway.get(chunk.key, file_obj.replica_count)
                f_obj = open(tmpfl, 'ab')
                f_obj.seek(chunk.seek)
                f_obj.write(data[:chunk.size])
                f_obj.close()
        except Exception, err:
            os.unlink(tmpfl)
            raise err

        return FileIterator(tmpfl)


    def __save_file(self, file_obj, file_path):
        f_obj = open(file_path, 'rb')
        seek = 0
        while True:
            data = f_obj.read(CHUNK_SIZE)
            size = len(data)
            if size == 0:
                break
            key, checksum = self.fabnet_gateway.put(data, replica_count=file_obj.replica_count)
            chunk = ChunkMD(key, checksum, seek, size)
            file_obj.chunks.append(chunk)
            seek += size



    def __get_metadata(self, reload_force=False):
        if self.metadata and not reload_force:
            return self.metadata

        user_id = self.security_provider.get_user_id()
        metadata_key = hashlib.sha1(user_id).hexdigest()
        metadata = self.fabnet_gateway.get(metadata_key)
        if metadata is None:
            raise Exception('No metadata found!')

        mdf = MetadataFile()
        mdf.load(metadata)
        self.metadata = mdf
        return self.metadata

    def __save_metadata(self):
        metadata = self.metadata.dump()
        user_id = self.security_provider.get_user_id()
        metadata_key = hashlib.sha1(user_id).hexdigest()
        try:
            self.fabnet_gateway.put(metadata, key=metadata_key)
        except Exception, err:
            self.__get_metadata(reload_force=True)
            raise err

    def register_user(self):
        if self.metadata:
            #user is already registered
            return

        user_id = self.security_provider.get_user_id()
        metadata_key = hashlib.sha1(user_id).hexdigest()
        metadata = self.fabnet_gateway.get(metadata_key)
        if metadata is not None:
            #user is already registered
            return

        mdf = MetadataFile()
        mdf.load('{}')
        self.fabnet_gateway.put(mdf.dump(), key=metadata_key)
        self.metadata = mdf


    def listdir(self, path='/'):
        mdf = self.__get_metadata()
        dir_obj = mdf.find(path)
        if not dir_obj.is_dir():
            raise Exception('%s is a file!'%path)

        return dir_obj.items()

    def mkdir(self, path, recursive=False):
        mdf = self.__get_metadata()
        if mdf.exists(path):
            raise Exception('Directory is already exists!'%path)

        base_path, new_dir = os.path.split(path)

        if not mdf.exists(base_path):
            if recursive:
                self.mkdir(base_path, recursive)
            else:
                raise Exception('Directory "%s" does not exists!'%base_path)

        base_path_obj = mdf.find(base_path)
        new_dir_obj = DirectoryMD(new_dir)
        base_path_obj.append(new_dir_obj)
        self.__save_metadata()

    def rmdir(self, path, recursive=False):
        mdf = self.__get_metadata()

        dir_obj = mdf.find(path)
        if not dir_obj.is_dir():
            raise Exception('%s is a file!'%path)


        items = dir_obj.items()
        if items and not recursive:
            raise Exception('Directory "%s" is not empty!'%path)

        for item in items:
            full_path = os.path.join(path, item[0])
            if item[1]:
                self.remove_file(full_path)
            else:
                self.rmdir(full_path, recursive)

        base_path, rm_dir = os.path.split(path)
        base_dir = mdf.find(base_path)
        base_dir.remove(rm_dir)
        self.__save_metadata()


    def save_file(self, file_path, dest_dir):
        if not os.path.exists(file_path):
            raise Exception('File %s does not found!'%file_path)

        mdf = self.__get_metadata()

        dir_obj = mdf.find(dest_dir)
        if not dir_obj.is_dir():
            raise Exception('%s is a file!'%dest_dir)

        file_name = os.path.basename(file_path)
        file_size = os.stat(file_path).st_size

        file_md = FileMD(file_name, file_size)

        self.__save_file(file_md, file_path)

        dir_obj.append(file_md)
        self.__save_metadata()

    def load_file(self, file_path):
        mdf = self.__get_metadata()
        if not mdf.exists(file_path):
            raise Exception('File %s does not found!'%file_path)

        file_obj = mdf.find(file_path)
        if not file_obj.is_file():
            raise Exception('%s is not a file!'%file_path)

        file_iter = self.__get_file(file_obj)
        return file_iter

    def remove_file(self, file_path):
        mdf = self.__get_metadata()
        if not mdf.exists(file_path):
            raise Exception('File %s does not found!'%file_path)

        parent_dir, file_name = os.path.split(file_path)
        dir_obj = mdf.find(parent_dir)

        dir_obj.remove(file_name)
        self.__save_metadata()
