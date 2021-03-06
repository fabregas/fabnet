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

from fabnet.utils.logger import oper_logger as logger
from fabnet.utils.internal import total_seconds
from fabnet.dht_mgmt.constants import MIN_HASH, MAX_HASH
from fabnet.core.config import Config
from fabnet.dht_mgmt.data_block import DataBlockHeader
from fabnet.core.fri_base import FileBasedChunks

class FSHashRangesException(Exception):
    pass

class FSHashRangesNotFound(FSHashRangesException):
    pass

class FSHashRangesNoData(FSHashRangesException):
    pass

class FSHashRangesOldDataDetected(FSHashRangesException):
    pass

class FSHashRangesPermissionDenied(FSHashRangesException):
    pass

class FSHashRangesNoFreeSpace(FSHashRangesException):
    pass


class TmpFile:
    def __init__(self, f_path, data, seek=0):
        self.__f_path = f_path
        self.__checksum = hashlib.sha1()
        self.__link_idx = 1

        if type(data) not in (list, tuple):
            data = [data]

        try:
            fobj = open(self.__f_path, 'wb')
        except IOError, err:
            raise FSHashRangesException('Cant create tmp file. Details: %s'%err)

        try:
            if seek:
                fobj.write('\x00'*seek)

            for data_block in data:
                if type(data_block) == str:
                    self.__int_write(fobj, data_block)
                else:
                    while True:
                        chunk = data_block.get_next_chunk()
                        if chunk is None:
                            break
                        self.__int_write(fobj, chunk)
        except IOError, err:
            raise FSHashRangesException('Cant save tmp data to file system. Details: %s'%err)
        except Exception, err:
            raise err
        finally:
            fobj.close()

    def hardlink(self):
        link = '%s.%s'%(self.__f_path, self.__link_idx)
        os.link(self.__f_path, link)
        self.__link_idx += 1
        return link

    def __int_write(self, fobj, data):
        self.__checksum.update(data)
        fobj.write(data)

    def __del__(self):
        self.remove()

    def checksum(self):
        return self.__checksum.hexdigest()

    def write(self, data, seek=None):
        try:
            fobj = open(self.__f_path, 'r+b')
        except IOError, err:
            raise FSHashRangesException('Cant create tmp file. Details: %s'%err)

        try:
            if seek is not None:
                fobj.seek(seek, 0)
            else:
                fobj.seek(0, 2) #seek to EOF

            self.__int_write(fobj, data)
        finally:
            fobj.close()

    def file_path(self):
        return self.__f_path

    def chunks(self):
        return FileBasedChunks(self.__f_path)

    def remove(self):
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

        self.__last_range_file = os.path.join(save_path, '.last_range')

        self.__child_ranges = SafeList()
        self.__parallel_writes = SafeCounter()
        self.__block_flag = threading.Event()
        self.__no_free_space_flag = threading.Event()
        self.__move_lock = threading.Lock()
        self.__ret_range_i = None

    def save_range(self):
        open(self.__last_range_file, 'w').write('%i %i'%(self.__start, self.__end))

    def get_last_range(self):
        if not os.path.exists(self.__last_range_file):
            return None
        data = open(self.__last_range_file).read()
        start, end = data.split()
        return int(start), int(end)


    def mktemp(self, binary_data):
        if not binary_data:
            return None

        tmp_file = self.tempfile()
        return TmpFile(tmp_file, binary_data)

    def tempfile(self):
        if self.__no_free_space_flag.is_set():
            if self.get_free_size_percents() > Config.CRITICAL_FREE_SPACE_PERCENT:
                self.__no_free_space_flag.clear()
                logger.info('Range is unlocked for write...')
            else:
                raise FSHashRangesNoFreeSpace('No free space for saving data block')

        return  os.path.join(self.__tmp_dir, hashlib.sha1(datetime.utcnow().isoformat()).hexdigest())

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

    def __check_ex_data_block(self, old_f_name, new_f_name):
        if os.path.exists(old_f_name):
            logger.debug('Checking data block datetime at %s...'%old_f_name)
            f_obj = open(old_f_name, 'rb')
            try:
                stored_header = f_obj.read(DataBlockHeader.HEADER_LEN)
            finally:
                f_obj.close()

            f_obj = open(new_f_name, 'rb')
            try:
                new_header = f_obj.read(DataBlockHeader.HEADER_LEN)
            finally:
                f_obj.close()

            try:
                _, _, _, user_id, stored_dt = DataBlockHeader.unpack(stored_header)
            except Exception, err:
                logger.error('Bad local data block header. %s'%err)
                return #we can store newer data block

            _, _, _, new_user_id, new_dt = DataBlockHeader.unpack(new_header)

            if new_dt < stored_dt:
                raise FSHashRangesOldDataDetected('Data block is already saved with newer datetime')
            if new_user_id != user_id:
                raise FSHashRangesPermissionDenied('Alien data block')


    def __put_data(self, key, tmp_file_path, save_to_reservation=False, save_to_replicas=False, check_dt=False):
        key = self._str_key(key)
        need_lock = False

        if save_to_replicas:
            range_dir = self.__replica_dir
        elif self.__block_flag.is_set():
            range_dir = self.__reservation_dir
        else:
            if save_to_reservation:
                range_dir = self.__reservation_dir
            else:
                range_dir = self.__range_dir
                need_lock = True

        dest = os.path.join(range_dir, key)
        if check_dt:
            self.__check_ex_data_block(dest, tmp_file_path)

        if need_lock:
            self.__parallel_writes.inc()

        try:
            shutil.move(tmp_file_path, dest)
        finally:
            if need_lock:
                self.__parallel_writes.dec()

    def __get_data_path(self, key, is_replica=False):
        key = self._str_key(key)
        if is_replica:
            f_name = os.path.join(self.__replica_dir, key)
            if not os.path.exists(f_name):
                raise FSHashRangesNoData('No replica data found for key %s'% key)
            return f_name

        file_path = os.path.join(self.__range_dir, key)
        if not os.path.exists(file_path):
            #check reservation range
            file_path = os.path.join(self.__reservation_dir, key)
            if not os.path.exists(file_path):
                raise FSHashRangesNoData('No data found for key %s'% key)

        return file_path

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



    def put(self, key, tmp_file_path, is_replica=False, check_dt=False):
        if is_replica:
            self.__put_data(key, tmp_file_path, save_to_replicas=True, check_dt=check_dt)
            return

        if self.__child_ranges.size():
            for child_range in self.__child_ranges.copy():
                if not child_range._in_range(key):
                    continue
                return child_range.put(key, tmp_file_path, check_dt)

        if self._in_range(key):
            self.__put_data(key, tmp_file_path, check_dt=check_dt)
        else:
            self.__put_data(key, tmp_file_path, save_to_reservation=True, check_dt=check_dt)


    def get_path(self, key, is_replica=False):
        if is_replica:
            return self.__get_data_path(key, is_replica)

        if self.__child_ranges.size():
            for child_range in self.__child_ranges.copy():
                if not child_range._in_range(key):
                    continue
                try:
                    return child_range.get_path(key)
                except FSHashRangesNoData, err:
                    break

        return self.__get_data_path(key)

    def get(self, key, is_replica=False):
        '''WARNING: This method can fail when runned across split/join pocess'''
        file_path = self.get_path(key, is_replica)
        return FileBasedChunks(file_path)

    def delete_data_block(self, key, is_replica, r_user_id, carefully_delete):
        file_path = self.get_path(key, is_replica)

        if carefully_delete:
            data = FileBasedChunks(file_path)
            header = data.read(DataBlockHeader.HEADER_LEN)
            _, _, checksum, user_id, _ = DataBlockHeader.unpack(header)
            if user_id != r_user_id:
                raise FSHashRangesPermissionDenied('Can not delete alien data block!')
        
        os.remove(file_path)

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
                yield digest, self.get(digest)
        except Exception, err:
            self._unblock_range()
            raise err

    def _ensure_not_write(self, file_path):
        f_dm = datetime.fromtimestamp(os.path.getmtime(file_path))
        if total_seconds(datetime.now() - f_dm) > Config.WAIT_FILE_MD_TIMEDELTA:
            return True
        return False

    def __iter_data_blocks(self, proc_dir, foreign_only=False):
        self.__move_lock.acquire()
        try:
            files = os.listdir(proc_dir)
            for digest in files:
                if foreign_only and self._in_range(digest):
                    continue
                file_path = os.path.join(proc_dir, digest)
                if self._ensure_not_write(file_path):
                    yield digest, FileBasedChunks(file_path), file_path
        finally:
            self.__move_lock.release()

    def iter_reservation(self):
        return self.__iter_data_blocks(self.__reservation_dir)

    def iter_replicas(self, foreign_only=True):
        return self.__iter_data_blocks(self.__replica_dir, foreign_only)

    def iter_data_blocks(self):
        return self.__iter_data_blocks(self.__range_dir)

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
        try:
            stat = os.stat(file_path)
        except OSError:
            #no file found
            return 0
        rest = stat.st_size % stat.st_blksize
        if rest:
            rest = stat.st_blksize - rest
        return stat.st_size + rest

    def get_range_size(self):
        return sum([self.__get_file_size(os.path.join(self.__range_dir, f)) for f in os.listdir(self.__range_dir)])

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

