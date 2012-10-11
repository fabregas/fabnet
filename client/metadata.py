#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package client.metadata
@author Konstantin Andrusenko
@date October 12, 2012

This module contains the implementation of user metadata classes.
"""
import json
from client.constants import DEFAULT_REPLICA_COUNT


class ChunkMD:
    def __init__(self, key=None, checksum=None, seek=None, size=None):
        self.checksum = checksum
        self.key = key
        self.seek = seek
        self.size = size

    def load(self, chunk_obj):
        self.checksum = chunk_obj.get('checksum', None)
        self.key = chunk_obj.get('key', None)
        self.seek = chunk_obj.get('seek', None)
        self.size = chunk_obj.get('size', None)

        if self.checksum is None:
            raise Exception('Bad metadata. Chunk checksum does not found!')
        if self.key is None:
            raise Exception('Bad metadata. Chunk key does not found!')
        if self.seek is None:
            raise Exception('Bad metadata. Chunk seek does not found!')
        if self.size is None:
            raise Exception('Bad metadata. Chunk size does not found!')

    def dump(self):
        return {'checksum': self.checksum,
                'key': self.key,
                'seek': self.seek,
                'size': self.size}


class FileMD:
    def __init__(self, name=None, size=0, replica_count=DEFAULT_REPLICA_COUNT):
        self.name = name
        self.size = size
        self.replica_count = replica_count
        self.chunks = []

    def is_dir(self):
        return False

    def is_file(self):
        return True

    def load(self, file_obj):
        self.name = file_obj.get('name', None)
        self.size = file_obj.get('size', None)
        self.chunks = file_obj.get('chunks', [])
        self.replica_count = file_obj.get('replica_count', DEFAULT_REPLICA_COUNT)

        if self.name is None:
            raise Exception('Bad metadata. File name does not found!')
        if self.size is None:
            raise Exception('Bad metadata. File size does not found!')
        if self.replica_count is None:
            raise Exception('Bad metadata. File replica count does not found!')

    def dump(self):
        return {'name': self.name,
                'size': self.size,
                'chunks': [c.dump() for c in self.chunks],
                'replica_count': self.replica_count }


class DirectoryMD:
    def __init__(self, name=''):
        self.name = name
        self.content = []

    def is_dir(self):
        return True

    def is_file(self):
        return False

    def load(self, dir_obj):
        self.name = dir_obj.get('name', None)
        if self.name is None:
            raise Exception('Bad metadata. Directory name does not found!')

        content = dir_obj.get('content', None)
        if content is None:
            raise Exception('Bad metadata. Directory content does not found!')

        for item in content:
            if item.has_key('content'):
                item_md = DirectoryMD()
            else:
                item_md = FileMD()
            item_md.load(item)
            self.content.append(item_md)

    def dump(self):
        return {'name': self.name,
                'content': [c.dump() for c in self.content]}

    def items(self):
        ret_items = []
        for item in self.content:
            ret_items.append((item.name, item.is_file()))

        return ret_items

    def get(self, item_name):
        for item in self.content:
            if item.name == item_name:
                return item

        raise Exception('"%s" does not found in %s directory'%(item_name, self.name))

    def append(self, item_md):
        if not isinstance(item_md, DirectoryMD) and not isinstance(item_md, FileMD):
            raise Exception('Item cant be appended to directory, bcs it type is equal to "%s"'%item_md)

        self.content.append(item_md)

    def remove(self, item_name):
        rm_i = None
        for i, item in enumerate(self.content):
            if item.name == item_name:
                rm_i = i
                break

        if rm_i is not None:
            del self.content[rm_i]


class MetadataFile:
    def __init__(self):
        self.versions = []
        self.root_dir = None

    def load(self, md_str):
        md_obj = json.loads(md_str)
        self.versions = md_obj.get('versions', [])
        root_dir = md_obj.get('root_dir', DirectoryMD().dump())

        self.root_dir = DirectoryMD()
        self.root_dir.load(root_dir)

    def dump(self):
        d_obj = {'versions': self.versions,
                 'root_dir': self.root_dir.dump()}

        return json.dumps(d_obj)

    def find(self, path):
        items = path.split('/')
        cur_item = self.root_dir
        for item_name in items:
            if not item_name:
                continue

            if not cur_item.is_dir():
                raise Exception('Path "%s" does not found!'%path)

            cur_item = cur_item.get(item_name)

        return cur_item

    def exists(self, path):
        try:
            self.find(path)
        except Exception, err:
            return False

        return True

