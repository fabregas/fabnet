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
        f_obj = tempfile.NamedTemporaryFile(prefix='nibbler-download-')
        try:
            for chunk in file_obj.chunks:
                data = self.fabnet_gateway.get(chunk.key, file_obj.replica_count)

                f_obj.seek(chunk.seek)
                f_obj.write(data[:chunk.size])
        except Exception, err:
            f_obj.close()
            raise err

        f_obj.seek(0)
        return f_obj


    def __save_file(self, file_obj, file_path):
        f_obj = open(file_path, 'rb')
        seek = 0
        try:
            while True:
                data = f_obj.read(CHUNK_SIZE)
                size = len(data)
                if size == 0:
                    break
                key, checksum = self.fabnet_gateway.put(data, replica_count=file_obj.replica_count)
                chunk = ChunkMD(key, checksum, seek, size)
                file_obj.chunks.append(chunk)
                seek += size
        finally:
            f_obj.close()



    def __get_metadata(self, reload_force=False, metadata_key=None):
        if self.metadata and not reload_force:
            return self.metadata

        if not metadata_key:
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
        user_id = self.security_provider.get_user_id()
        version_key = self.metadata.make_new_version(user_id)
        metadata = self.metadata.dump()
        try:
            self.fabnet_gateway.put(metadata, key=version_key)
        except Exception, err:
            self.metadata.remove_version(version_key)

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

    def get_resource(self, path):
        mdf = self.__get_metadata()
        try:
            path_obj = mdf.find(path)
            return path_obj
        except PathException, err:
            #print 'get_resource: ', err
            return None

    def get_versions(self):
        mdf = self.__get_metadata()
        return mdf.get_versions()

    def load_version(self, version_key):
        self.__get_metadata(reload_force=True, metadata_key=version_key)

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


    def save_file(self, file_path, file_name, dest_dir):
        if file_path and not os.path.exists(file_path):
            raise Exception('File %s does not found!'%file_path)

        mdf = self.__get_metadata()

        dir_obj = mdf.find(dest_dir)
        if not dir_obj.is_dir():
            raise Exception('%s is a file!'%dest_dir)

        if file_path:
            file_size = os.stat(file_path).st_size
        else:
            file_size = 0

        if isinstance(file_name, FileMD):
            file_md = file_name
            file_md.size = file_size
        else:
            file_md = FileMD(file_name, file_size)

        empty = (file_size == 0)
        if not empty:
            print 'SAVING %s'%file_md.name
            self.__save_file(file_md, file_path)

        dir_obj.append(file_md)

        if not empty:
            self.__save_metadata()

    def load_file(self, file_path):
        if isinstance(file_path, FileMD):
            file_obj = file_path
        else:
            mdf = self.__get_metadata()
            if not mdf.exists(file_path):
                raise Exception('File %s does not found!'%file_path)
            file_obj = mdf.find(file_path)

        if not file_obj.is_file():
            raise Exception('%s is not a file!'%file_path)

        return self.__get_file(file_obj)

    def move(self, s_path, d_path):
        print 'mv %s to %s'%(s_path, d_path)
        mdf, d_obj, source, new_name, dst_path = self._cpmv_int(s_path, d_path)

        base_path, s_name = os.path.split(s_path)
        mdf.find(base_path).remove(s_name)

        if new_name:
            source.name = new_name

        d_obj.append(source)
        self.__save_metadata()

    def copy(self, s_path, d_path):
        print 'cp %s to %s'%(s_path, d_path)
        mdf, d_obj, source, new_name, dst_path = self._cpmv_int(s_path, d_path)
        if not new_name:
            new_name = source.name

        if source.is_file():
            fhdl = self.load_file(s_path)
            try:
                self.save_file(fhdl.name, new_name, dst_path)
            finally:
                fhdl.close()
        else:
            dst_dir = os.path.join(dst_path, new_name)
            self.mkdir(dst_dir)
            for i_name, dummy in source.items():
                self.copy(os.path.join(s_path, i_name), dst_dir)

        self.__save_metadata()

    def _cpmv_int(self, s_path, d_path):
        mdf = self.__get_metadata()
        if not mdf.exists(s_path):
            raise Exception('Path %s does not found!'%s_path)

        if mdf.exists(d_path):
            new_name = None
            dst_path = d_path
            d_obj = mdf.find(d_path)
            if d_obj.is_file():
                raise Exception('File %s is already exists!'%d_path)
        else:
            dst_path, new_name = os.path.split(d_path)
            if not mdf.exists(dst_path):
                raise Exception('Directory %s does not found!'%dst_path)
            d_obj = mdf.find(dst_path)

        source = mdf.find(s_path)
        return mdf, d_obj, source, new_name, dst_path


    def remove_file(self, file_path):
        mdf = self.__get_metadata()
        if not mdf.exists(file_path):
            raise Exception('File %s does not found!'%file_path)

        parent_dir, file_name = os.path.split(file_path)
        dir_obj = mdf.find(parent_dir)

        dir_obj.remove(file_name)
        self.__save_metadata()
