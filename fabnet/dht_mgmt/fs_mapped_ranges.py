#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.dht_mgmt.fs_mapped_ranges

@author Konstantin Andrusenko
@date September 15, 2012
"""
import os
import shutil
import threading
import hashlib
import copy
import time
from datetime import datetime

from fabnet.utils.logger import logger
from fabnet.utils.internal import total_seconds
from fabnet.dht_mgmt.constants import MIN_HASH, MAX_HASH
from fabnet.core.config import Config
from fabnet.dht_mgmt.data_block import DataBlockHeader
from fabnet.core.fri_base import FriBinaryData
from fabnet.core.constants import DEFAULT_CHUNK_SIZE

class FSHashRangesException(Exception):
    pass

class FSHashRangesNotFound(FSHashRangesException):
    pass

class FSHashRangesNoData(FSHashRangesException):
    pass

class FSHashRangesOldDataDetected(FSHashRangesException):
    pass

class FSHashRangesNoFreeSpace(FSHashRangesException):
    pass



class FileBasedChunks(FriBinaryData):
    def __init__(self, file_path, chunk_size=DEFAULT_CHUNK_SIZE):
        self.__file_path = file_path
        self.__chunk_size = chunk_size
        self.__f_obj = None
        self.__no_data_flag = False
        self.__read_bytes = 0

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
                self.__eof()
                return None
            self.__read_bytes += len(chunk)
            return chunk
        except IOError, err:
            self.__eof()
            raise FSHashRangesException('Cant read data from file system. Details: %s'%err)
        except Exception, err:
            self.__eof()
            raise err

    def get_next_chunk(self):
        return self.read(self.__chunk_size)


    def __eof(self):
        self.__no_data_flag = True
        if self.__f_obj:
            self.__f_obj.close()


class TmpFile:
    def __init__(self, f_path, binary_data):
        self.__f_path = f_path

        try:
            fobj = open(self.__f_path, 'wb')
        except IOError, err:
            raise FSHashRangesException('Cant create tmp file. Details: %s'%err)

        try:
            while True:
                chunk = binary_data.get_next_chunk()

                if chunk is None:
                    break

                fobj.write(chunk)
        except IOError, err:
            os.unlink(self.__f_path)
            raise FSHashRangesException('Cant save tmp data to file system. Details: %s'%err)
        except Exception, err:
            os.unlink(self.__f_path)
            raise err
        finally:
            fobj.close()

    def chunks(self):
        return FileBasedChunks(self.__f_path)

    def __del__(self):
        if os.path.exists(self.__f_path):
            os.unlink(self.__f_path)


class SafeCounter:
    def __init__(self):
        self.__count = 0
        self.__lock = threading.Lock()

    def inc(self):
        self.__lock.acquire()
        try:
            self.__count += 1
        finally:
            self.__lock.release()

    def dec(self):
        self.__lock.acquire()
        try:
            self.__count -= 1
        finally:
            self.__lock.release()

    def count(self):
        self.__lock.acquire()
        try:
            return self.__count
        finally:
            self.__lock.release()

class SafeList:
    def __init__(self):
        self.__list = []
        self.__lock = threading.Lock()

    def append(self, value):
        self.__lock.acquire()
        try:
            self.__list.append(value)
        finally:
            self.__lock.release()

    def get(self, num):
        self.__lock.acquire()
        try:
            return self.__list[num]
        finally:
            self.__lock.release()

    def concat(self, app_list):
        self.__lock.acquire()
        try:
            self.__list += app_list
        finally:
            self.__lock.release()

    def copy(self):
        self.__lock.acquire()
        try:
            return copy.copy(self.__list)
        finally:
            self.__lock.release()

    def size(self):
        self.__lock.acquire()
        try:
            return len(self.__list)
        finally:
            self.__lock.release()

    def clear(self):
        self.__lock.acquire()
        try:
            for_ret = self.__list
            self.__list = []

            return for_ret
        finally:
            self.__lock.release()


class FSHashRanges:
    @staticmethod
    def discovery_range(save_path, ret_full=False):
        items = os.listdir(save_path)

        discovered_ranges = []
        max_range = None
        for item in items:
            parts = item.split('_')
            if len(parts) != 2:
                continue

            try:
                start_key = long(parts[0], 16)
            except ValueError:
                continue

            try:
                end_key = long(parts[1], 16)
            except ValueError:
                continue

            hash_range = FSHashRanges(start_key, end_key, save_path)
            if not max_range:
                max_range = hash_range
            elif hash_range.length() > max_range.length():
                max_range = hash_range

            discovered_ranges.append(hash_range)

        for h_range in discovered_ranges:
            if not ret_full and h_range == max_range:
                continue
            h_range.move_to_reservation()

        if ret_full or (not max_range):
            max_range = FSHashRanges(MIN_HASH, MAX_HASH, save_path)

        if max_range:
            max_range.restore_from_reservation()

        return max_range


    def __init__(self, start, end, save_path):
        self.__start = self._long_key(start)
        self.__end = self._long_key(end)
        self.__save_path = save_path

        dir_name = '%040x_%040x'%(self.__start, self.__end)
        self.__range_dir = os.path.join(save_path, dir_name)
        if not os.path.exists(self.__range_dir):
            try:
                os.mkdir(self.__range_dir)
            except OSError, err:
                raise FSHashRangesException('Cant create directory for range: %s'%self.__range_dir)

        self.__reservation_dir = os.path.join(save_path, 'reservation_range')
        if not os.path.exists(self.__reservation_dir):
            try:
                os.mkdir(self.__reservation_dir)
            except OSError, err:
                raise FSHashRangesException('Cant create directory for range: %s'%self.__reservation_dir)

        self.__replica_dir = os.path.join(save_path, 'replica_data')
        if not os.path.exists(self.__replica_dir):
            os.mkdir(self.__replica_dir)

        self.__tmp_dir = os.path.join(save_path, 'tmp')
        if not os.path.exists(self.__tmp_dir):
            os.mkdir(self.__tmp_dir)

        self.__child_ranges = SafeList()
        self.__parallel_writes = SafeCounter()
        self.__block_flag = threading.Event()
        self.__no_free_space_flag = threading.Event()
        self.__move_lock = threading.Lock()
        self.__ret_range_i = None

    def block_for_write(self):
        self.__no_free_space_flag.set()

    def get_start(self):
        return self.__start

    def get_end(self):
        return self.__end

    def length(self):
        return self.__end - self.__start

    def is_max_range(self):
        if self.__start == MIN_HASH and self.__end == MAX_HASH:
            return True
        return False

    def get_range_dir(self):
        return self.__range_dir

    def get_replicas_dir(self):
        return self.__replica_dir

    def is_equal(self, another_range):
        if self.__start == another_range.get_start() and \
            self.__end == another_range.get_end():
            return True
        return False

    def _in_range(self, key):
        if self.__start <= self._long_key(key) <= self.__end:
            return True
        return False

    def _move_from(self, file_path, rewrite=True):
        dest = os.path.join(self.__range_dir, os.path.basename(file_path))
        if os.path.exists(dest):
            if not rewrite:
                os.remove(file_path)
                return
            os.remove(dest)

        shutil.move(file_path, self.__range_dir)

    def _wait_write_buffers(self):
        while self.__parallel_writes.count():
            logger.info('Waiting swapping buffers to disk...')
            time.sleep(1)

    def _long_key(self, key):
        if type(key) == int:
            key = long(key)
        elif type(key) != long:
            key = long(key, 16)

        return key

    def _str_key(self, key):
        if type(key) in (int, long):
            return '%040x'%key
        return key

    def __check_ex_data_block(self, f_name, data):
        if os.path.exists(f_name):
            logger.debug('Checking data block datetime at %s...'%f_name)
            f_obj = open(f_name, 'rb')
            try:
                stored_header = f_obj.read(DataBlockHeader.HEADER_LEN)

                try:
                    _, _, _, stored_dt = DataBlockHeader.unpack(stored_header)
                except Exception, err:
                    logger.error('Bad local data block header. %s'%err)
                    return #we can store newer data block

                _, _, _, new_dt = DataBlockHeader.unpack(data)

                if new_dt < stored_dt:
                    raise FSHashRangesOldDataDetected('Data block is already saved with newer datetime')
            finally:
                f_obj.close()

    def __write_data(self, f_name, data, check_dt=False):
        if type(data) not in (list, tuple):
            data = [data]

        try:
            if self.__no_free_space_flag.is_set():
                if self.get_free_size_percents() > Config.CRITICAL_FREE_SPACE_PERCENT:
                    self.__no_free_space_flag.clear()
                    logger.info('Range is unlocked for write...')
                else:
                    raise FSHashRangesNoFreeSpace('No free space for saving data block')


            rest_chunk = ''
            if check_dt:
                data_block = data[0]
                if type(data_block) != str:
                    data_block = data_block.get_next_chunk()
                    rest_chunk = data_block

                self.__check_ex_data_block(f_name, data_block)

            f_obj = open(f_name, 'wb')
            try:
                for data_block in data:
                    if type(data_block) == str:
                        f_obj.write(data_block)
                    else:
                        if rest_chunk:
                            f_obj.write(rest_chunk)

                        while True:
                            chunk = data_block.get_next_chunk()
                            if chunk is None:
                                break
                            f_obj.write(chunk)
            finally:
                f_obj.close()
        except IOError, err:
            raise FSHashRangesException('Cant save data to file system. Details: %s'%err)


    def __read_data(self, file_path, header_only=False):
        if not header_only:
            return FileBasedChunks(file_path)

        f_obj = open(file_path, 'rb')
        try:
            data = f_obj.read(DataBlockHeader.HEADER_LEN)
        except IOError, err:
            raise FSHashRangesException('Cant read data from file system. Details: %s'%err)
        finally:
            f_obj.close()

        return data

    def __put_data(self, key, data, save_to_reservation=False, check_dt=False):
        key = self._str_key(key)
        if self.__block_flag.is_set():
            range_dir = self.__reservation_dir
            is_reserv = True
        else:
            if save_to_reservation:
                range_dir = self.__reservation_dir
            else:
                range_dir = self.__range_dir

            self.__parallel_writes.inc()
            is_reserv = False

        try:
            self.__write_data(os.path.join(range_dir, key), data, check_dt)
        finally:
            if not is_reserv:
                self.__parallel_writes.dec()

    def __get_data(self, key, header_only=False):
        key = self._str_key(key)
        file_path = os.path.join(self.__range_dir, key)
        if not os.path.exists(file_path):
            #check reservation range
            file_path = os.path.join(self.__reservation_dir, key)
            if not os.path.exists(file_path):
                raise FSHashRangesNoData('No data found for key %s'% key)

        return self.__read_data(file_path, header_only)

    def __join_data(self, child_ranges):
        for child_range in child_ranges:
            child_range_dir = child_range.get_range_dir()
            if not os.path.exists(child_range_dir):
                continue
            files = os.listdir(child_range_dir)
            perc_part = len(files)/10

            logger.info('Joining range %s...'%child_range_dir)

            for cnt, digest in enumerate(files):
                if perc_part and (cnt+1) % perc_part == 0:
                    perc = (cnt+1)/perc_part
                    if perc <= 10:
                        logger.info('Joining range progress: %i'%perc + '0%...')

                self._move_from(os.path.join(child_range_dir, digest))

            logger.info('Range is joined!')


    def __split_data(self):
        files = os.listdir(self.__range_dir)
        perc_part = len(files)/10

        logger.info('Splitting hash range...')

        for cnt, digest in enumerate(files):
            if perc_part and (cnt+1) % perc_part == 0:
                perc = (cnt+1)/perc_part
                if perc <= 10:
                    logger.info('Splitting range progress: %i'%perc + '0%...')

            for child_range in self.__child_ranges.copy():
                if child_range._in_range(digest):
                    child_range._move_from(os.path.join(self.__range_dir, digest))
                    break

        logger.info('Range is splitted!')


    def _destroy(self, force=False):
        if not os.path.exists(self.__range_dir):
            return

        try:
            if force:
                shutil.rmtree(self.__range_dir)
            else:
                os.rmdir(self.__range_dir)
        except OSError, err:
            raise FSHashRangesException('Cant destroy ranges directory. Details: %s'%err)


    def _block_range(self):
        self.__block_flag.set()

    def _unblock_range(self):
        self.__block_flag.clear()



    def put(self, key, data, check_dt=False):
        if self.__child_ranges.size():
            for child_range in self.__child_ranges.copy():
                if not child_range._in_range(key):
                    continue
                return child_range.put(key, data)

        if self._in_range(key):
            self.__put_data(key, data, check_dt=check_dt)
        else:
            self.__put_data(key, data, save_to_reservation=True, check_dt=check_dt)

    def get(self, key):
        '''WARNING: This method can fail when runned across split/join pocess'''
        if self.__child_ranges.size():
            for child_range in self.__child_ranges.copy():
                if not child_range._in_range(key):
                    continue
                try:
                    data = child_range.get(key)
                    return data
                except FSHashRangesNoData, err:
                    break

        return self.__get_data(key)


    def put_replica(self, key, data, check_dt=False):
        key = self._str_key(key)
        f_name = os.path.join(self.__replica_dir, key)
        self.__write_data(f_name, data, check_dt)


    def get_replica(self, key):
        key = self._str_key(key)
        f_name = os.path.join(self.__replica_dir, key)
        if not os.path.exists(f_name):
            raise FSHashRangesNoData('No replica data found for key %s'% key)

        return self.__read_data(f_name)


    def extend(self, start_key, end_key):
        self.__move_lock.acquire()
        try:
            start = self.__start
            end = self.__end
            start_key = self._long_key(start_key)
            end_key = self._long_key(end_key)
            if start_key >= end_key:
                raise FSHashRangesException('Bad subrange [%040x-%040x] of [%040x-%040x]'%\
                                            (start_key, end_key, self.__start, self.__end))

            if self.__start == end_key+1:
                start = start_key
            elif self.__end == start_key-1:
                end = end_key
            else:
                raise FSHashRangesException('Bad range for extend [%040x-%040x] of [%040x-%040x]'%\
                                            (start_key, end_key, self.__start, self.__end))

            self._block_range()
            self._wait_write_buffers()
            h_range = FSHashRanges(start, end, self.__save_path)
            try:
                self.move_to_reservation()
            except Exception, err:
                self.restore_from_reservation()
                self._unblock_range()
                raise err

            h_range.restore_from_reservation()
        finally:
            self.__move_lock.release()

        return h_range


    def split_range(self, start_key, end_key):
        self.__move_lock.acquire()
        try:
            start_key = self._long_key(start_key)
            end_key = self._long_key(end_key)
            if start_key == self.__start:
                split_key = end_key
                self.__ret_range_i = 0
                first_subrange_end = split_key
                second_subrange_start = split_key + 1
            elif end_key == self.__end:
                split_key = start_key
                self.__ret_range_i = 1
                first_subrange_end = split_key - 1
                second_subrange_start = split_key
            else:
                raise FSHashRangesException('Bad subrange [%040x-%040x] for range [%040x-%040x]'%\
                                                (start_key, end_key, self.__start, self.__end))

            if not self._in_range(split_key):
                FSHashRangesNotFound('No key %040x found in range'%split_key)

            first_rg = FSHashRanges(self.__start, first_subrange_end, self.__save_path)
            second_rg = FSHashRanges(second_subrange_start, self.__end, self.__save_path)

            self.__child_ranges.concat([first_rg, second_rg])

            self._wait_write_buffers()

            self.__split_data()
        finally:
            self.__move_lock.release()

        return self.get_subranges()


    def iter_range(self):
        self._block_range()
        self._wait_write_buffers()
        try:
            files = os.listdir(self.__range_dir)
            for digest in files:
                yield digest, self.__get_data(digest)
        except Exception, err:
            self._unblock_range()
            raise err

    def _ensure_not_write(self, file_path):
        f_dm = datetime.fromtimestamp(os.path.getmtime(file_path))
        if total_seconds(datetime.now() - f_dm) > Config.WAIT_FILE_MD_TIMEDELTA:
            return True
        return False

    def __iter_data_blocks(self, proc_dir, foreign_only=False, header_only=False):
        self.__move_lock.acquire()
        try:
            files = os.listdir(proc_dir)
            for digest in files:
                if foreign_only and self._in_range(digest):
                    continue
                file_path = os.path.join(proc_dir, digest)
                if self._ensure_not_write(file_path):
                    yield digest, self.__read_data(file_path, header_only), file_path
        finally:
            self.__move_lock.release()

    def iter_reservation(self, header_only=False):
        return self.__iter_data_blocks(self.__reservation_dir, header_only=header_only)

    def iter_replicas(self, foreign_only=True, header_only=False):
        return self.__iter_data_blocks(self.__replica_dir, foreign_only, header_only)

    def iter_data_blocks(self, header_only=False):
        return self.__iter_data_blocks(self.__range_dir, header_only=header_only)

    def join_subranges(self):
        self.__move_lock.acquire()
        try:
            child_ranges = self.__child_ranges.copy()
            if not child_ranges:
                return

            for child_range in child_ranges:
                child_range._block_range()

            for child_range in child_ranges:
                child_range._wait_write_buffers()

            self.__join_data(child_ranges)

            for child_range in child_ranges:
                child_range._destroy()

            self.__child_ranges.clear()

            self.__ret_range_i = None
        finally:
            self.__move_lock.release()


    def get_subranges(self):
        if self.__child_ranges.size():
            ranges = self.__child_ranges.copy()
            return ranges[self.__ret_range_i], ranges[int(not self.__ret_range_i)]

        return None

    def move_to_reservation(self):
        if not os.path.exists(self.__range_dir):
            return

        self._block_range()
        self._wait_write_buffers()
        files = os.listdir(self.__range_dir)
        for digest in files:
            file_path = os.path.join(self.__range_dir, digest)
            dest = os.path.join(self.__reservation_dir, digest)
            if os.path.exists(dest):
                os.remove(dest)

            shutil.move(file_path, dest)

        self._destroy()

    def restore_from_reservation(self):
        files = os.listdir(self.__reservation_dir)
        perc_part = len(files)/10

        logger.info('Restoring reservation data...')

        for cnt, digest in enumerate(files):
            if perc_part and (cnt+1) % perc_part == 0:
                perc = (cnt+1)/perc_part
                if perc <= 10:
                    logger.info('Restore progress: %i'%perc + '0%...')

            r_file_path = os.path.join(self.__reservation_dir, digest)
            if self._in_range(digest) and self._ensure_not_write(r_file_path):
                self._move_from(r_file_path, rewrite=False)

        logger.info('Data is restored from reservation!')

    def __get_file_size(self, file_path):
        stat = os.stat(file_path)
        rest = stat.st_size % stat.st_blksize
        if rest:
            rest = stat.st_blksize - rest
        return stat.st_size + rest

    def get_range_size(self):
        self.__move_lock.acquire()
        try:
            return sum([self.__get_file_size(os.path.join(self.__range_dir, f)) for f in os.listdir(self.__range_dir)])
        finally:
            self.__move_lock.release()

    def get_replicas_size(self):
        return sum([self.__get_file_size(os.path.join(self.__replica_dir, f)) for f in os.listdir(self.__replica_dir)])

    def get_all_related_data_size(self):
        range_size = self.get_range_size()
        replica_size = 0
        for digest in os.listdir(self.__replica_dir):
            if self._in_range(digest):
                replica_size += self.__get_file_size(os.path.join(self.__replica_dir, digest))

        return replica_size + range_size

    def get_free_size(self):
        stat = os.statvfs(self.__range_dir)
        free_space = stat.f_bsize * stat.f_bavail
        return free_space

    def get_free_size_percents(self):
        stat = os.statvfs(self.__range_dir)
        return (stat.f_bavail * 100.) / stat.f_blocks

    def get_estimated_data_percents(self, add_size=0):
        estimated_data_size = self.get_range_size() + self.get_replicas_size() + add_size
        stat = os.statvfs(self.__range_dir)
        estimated_data_size_perc = (estimated_data_size * 100.) / (stat.f_blocks * stat.f_bsize)
        return estimated_data_size_perc

    def mktemp(self, binary_data):
        if not binary_data:
            return None

        tmp_file = os.path.join(self.__tmp_dir, hashlib.sha1(datetime.utcnow().isoformat()).hexdigest())
        return TmpFile(tmp_file, binary_data)

