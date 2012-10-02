#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.hash_ranges_table

@author Konstantin Andrusenko
@date September 5, 2012
"""
import threading
import pickle
import copy
from sha import sha
from datetime import datetime

from fabnet.utils.logger import logger


class RangeException(Exception):
    pass

class HashRange:
    def __init__(self, start, end, node_address):
        self.start = start
        self.end = end
        self.node_address = node_address
        self.__length = self.end - self.start

    #def __repr__(self):
    #    return '(%s-%s)%s'%(self.start, self.end, self.node_address)

    def to_str(self):
        return '{%040x-%040x}-%s'%(self.start, self.end, self.node_address)

    def length(self):
        return self.__length


class HashRangesTable:
    def __init__(self):
        self.__ranges = []
        self.__lock = threading.RLock()
        self.__last_dm = datetime(1, 1, 1, 1, 1, 1, 1)
        self.__blocked = threading.Event()

    def is_blocked(self):
        return self.__blocked.is_set()

    def get_checksum(self):
        self.__lock.acquire()
        try:
            checksum = sha()
            for range_obj in self.__ranges:
                checksum.update(range_obj.to_str())
            return checksum.hexdigest(), self.__last_dm
        finally:
            self.__lock.release()

    def get_last_dm(self):
        return self.__last_dm

    def empty(self):
        self.__lock.acquire()
        try:
            return not bool(self.__ranges)
        finally:
            self.__lock.release()

    def append(self, start, end, node_addr):
        self.__lock.acquire()
        try:
            if self.__blocked.is_set():
                raise RangeException('Ranges table is blocked for write! Waiting neighbour arbitring...')

            r_obj = self.find(start)
            if r_obj:
                err_msg = 'Cant append range [%s-%s], it is crossed by existing [%s-%s] range' \
                            % (start, end, r_obj.start, r_obj.end)
                raise RangeException(err_msg)

            r_obj = self.find(end)
            if r_obj:
                err_msg = 'Cant append range [%s-%s], it is crossed by existing [%s-%s] range' \
                            % (start, end, r_obj.start, r_obj.end)
                raise RangeException(err_msg)

            h_range = HashRange(start, end, node_addr)
            self.__sorted_insert(h_range)
            self.__last_dm = datetime.utcnow()
        finally:
            self.__lock.release()

    def remove(self, ex_hash):
        self.__lock.acquire()
        try:
            if self.__blocked.is_set():
                raise RangeException('Ranges table is blocked for write! Waiting neighbour arbitring...')

            r_obj = self.find(ex_hash)
            if not r_obj:
                return None
                #raise RangeException('Range does not found for hash %s'%ex_hash)

            del self.__ranges[self.__ranges.index(r_obj)]
            self.__last_dm = datetime.utcnow()
        finally:
            self.__lock.release()

    def __sorted_insert(self, new_range_obj):
        tr_start = 0
        max_len = tr_end = len(self.__ranges)-1
        cur_n = int(tr_end/2)

        while True:
            if tr_start > tr_end:
                break

            if cur_n > 1:
                pre_obj = self.__ranges[cur_n-1]
            else:
                pre_obj = None

            if cur_n < max_len:
                next_obj = self.__ranges[cur_n+1]
            else:
                next_obj = None

            range_obj = self.__ranges[cur_n]
            if pre_obj and pre_obj.end < new_range_obj.start \
                    and range_obj.start > new_range_obj.end:
                break

            if next_obj and range_obj.end < new_range_obj.start \
                    and next_obj.start > new_range_obj.end:
                cur_n += 1
                break

            if new_range_obj.start > range_obj.end:
                tr_start = cur_n + 1
            else:
                tr_end = cur_n - 1

            cur_n = int((tr_start + tr_end)/2)
            pre_obj = range_obj

        if tr_start > tr_end:
            if tr_end < 0:
                self.__ranges.insert(0, new_range_obj)
            else:
                self.__ranges.append(new_range_obj)
        else:
            return self.__ranges.insert(cur_n, new_range_obj)


    def __repr__(self):
        return str(self.__ranges)

    def copy(self):
        self.__lock.acquire()
        try:
            return copy.copy(self.__ranges)
        finally:
            self.__lock.release()

    def dump(self):
        self.__lock.acquire()
        try:
            return pickle.dumps([self.__ranges, self.__last_dm])
        finally:
            self.__lock.release()

    def load(self, ranges_dump):
        self.__lock.acquire()
        try:
            self.__ranges, self.__last_dm = pickle.loads(ranges_dump)
            self.__blocked.clear()
        finally:
            self.__lock.release()

    def validate_changes(self, rm_obj_list, ap_obj_list):
        try:
            tmp_table = HashRangesTable()
            for rm_obj in rm_obj_list:
                tmp_table.append(rm_obj.start, rm_obj.end, '')

            end_i = None
            for item in tmp_table.iter_table():
                if end_i and item.start != end_i-1:
                    raise Exception('Ranges are not one near one')
                end_i = item.end

            for app_obj in ap_obj_list:
                if (tmp_table.get_first().start > app_obj.start and self.find(app_obj.start)) \
                        or (tmp_table.get_end().end < app_obj.end and self.find(app_obj.end)):
                    raise Exception('Appending range {%040x-%040x} is intersected by exists range!'%(app_obj.start, app_obj.end))

            for rm_obj in rm_obj_list:
                found = self.find(rm_obj.start)
                if found:
                    if rm_obj.start != found.start or rm_obj.end != found.end:
                        raise Exception('Removing range {%040x-%040x} is not found in ranges table')
                else:
                    found = self.find(rm_obj.end)
                    if found and (rm_obj.start != found.start or rm_obj.end != found.end):
                        raise Exception('Removing range {%040x-%040x} is not found in ranges table')
        except Exception, err:
            logger.info('Error in range table. Blocking it and wait arbitring...')
            self.__blocked.set()
            raise err


    def __find_int(self, find_value):
        tr_start = 0
        tr_end = len(self.__ranges)-1
        cur_n = int(tr_end/2)

        while True:
            if tr_start > tr_end:
                break

            range_obj = self.__ranges[cur_n]
            if range_obj.start <= find_value <= range_obj.end:
                break

            if find_value > range_obj.end:
                tr_start = cur_n + 1
            else:
                tr_end = cur_n - 1

            cur_n = int((tr_start + tr_end)/2)

        if tr_start > tr_end:
            return None
        else:
            return cur_n


    def find(self, find_value):
        self.__lock.acquire()
        try:
            idx = self.__find_int(find_value)
            if idx is not None:
                return self.__ranges[idx]
            return None
        finally:
            self.__lock.release()


    def find_next(self, find_value):
        self.__lock.acquire()
        try:
            idx = self.__find_int(find_value)
            if idx is not None and idx < (len(self.__ranges)-1):
                return self.__ranges[idx+1]
            return None
        finally:
            self.__lock.release()

    def get_first(self):
        self.__lock.acquire()
        try:
            if self.__ranges:
                return self.__ranges[0]
            return None
        finally:
            self.__lock.release()

    def get_end(self):
        self.__lock.acquire()
        try:
            if self.__ranges:
                return self.__ranges[-1]
            return None
        finally:
            self.__lock.release()

    def iter_table(self):
        self.__lock.acquire()
        try:
            for range_obj in self.__ranges:
                yield range_obj
        finally:
            self.__lock.release()



