import os
import shutil
import threading
import copy
import time

from fabnet.utils.logger import logger
from fabnet.dht_mgmt.constants import MIN_HASH, MAX_HASH

class FSHashRangesException(Exception):
    pass

class FSHashRangesNotFound(FSHashRangesException):
    pass

class FSHashRangesNoData(FSHashRangesException):
    pass

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
    def discovery_range(save_path):
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

        if not max_range:
            return FSHashRanges(MIN_HASH, MAX_HASH, save_path)

        for h_range in discovered_ranges:
            if h_range != max_range:
                h_range.move_to_trash()

        max_range.restore_from_trash()
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

        self.__trash_dir = os.path.join(save_path, 'trash')

        self.__child_ranges = SafeList()
        self.__parallel_writes = SafeCounter()
        self.__block_flag = threading.Event()
        self.__ret_range_i = None

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

    def __put_data(self, key, data, save_to_reservation=False):
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
            f_obj = open(os.path.join(range_dir, key), 'wb')
            try:
                f_obj.write(data)
            finally:
                f_obj.close()
        except IOError, err:
            raise FSHashRangesException('Cant save data to file system. Details: %s'%err)
        finally:
            if not is_reserv:
                self.__parallel_writes.dec()

    def __get_data(self, key):
        key = self._str_key(key)
        file_path = os.path.join(self.__range_dir, key)
        if not os.path.exists(file_path):
            #check reservation range
            file_path = os.path.join(self.__reservation_dir, key)
            if not os.path.exists(file_path):
                #check trash
                file_path = os.path.join(self.__trash_dir, key)
                if not os.path.exists(file_path):
                    raise FSHashRangesNoData('No data found for key %s'% key)

        f_obj = open(file_path, 'rb')
        try:
            data = f_obj.read()
        except IOError, err:
            raise FSHashRangesException('Cant read data from file system. Details: %s'%err)
        finally:
            f_obj.close()

        return data

    def __join_data(self, child_ranges):
        for child_range in child_ranges:
            child_range_dir = child_range.get_range_dir()
            if not os.path.exists(child_range_dir):
                continue
            files = os.listdir(child_range_dir)
            perc_part = len(files)/10

            logger.info('Joining range %s...'%child_range_dir)

            for cnt, digest in enumerate(files):
                if perc_part and (cnt) % perc_part == 0:
                    logger.info('Joining range progress: %i'%(cnt/perc_part) + '0%...')

                self._move_from(os.path.join(child_range_dir, digest))

            logger.info('Range is joined!')


    def __split_data(self):
        files = os.listdir(self.__range_dir)
        perc_part = len(files)/10

        logger.info('Splitting hash range...')

        for cnt, digest in enumerate(files):
            if perc_part and (cnt) % perc_part == 0:
                logger.info('Splitting range progress: %i'%(cnt/perc_part) + '0%...')

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



    def put(self, key, data):
        if self.__child_ranges.size():
            for child_range in self.__child_ranges.copy():
                if not child_range._in_range(key):
                    continue
                return child_range.put(key, data)

        if self._in_range(key):
            self.__put_data(key, data)
        else:
            self.__put_data(key, data, save_to_reservation=True)

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


    def split_range(self, start_key, end_key):
        start_key = self._long_key(start_key)
        end_key = self._long_key(end_key)
        if start_key == self.__start:
            split_key = end_key
            self.__ret_range_i = 0
        elif end_key == self.__end:
            split_key = start_key
            self.__ret_range_i = 1
        else:
            raise FSHashRangesException('Bad subrange [%s-%s]'%(start_key, end_key))

        if not self._in_range(split_key):
            FSHashRangesNotFound('No key %s found in range'%split_key)

        key_long = split_key
        first_rg = FSHashRanges(self.__start, key_long-1, self.__save_path)
        second_rg = FSHashRanges(key_long, self.__end, self.__save_path)

        self.__child_ranges.concat([first_rg, second_rg])

        self._wait_write_buffers()

        self.__split_data()

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


    def iter_reservation(self):
        files = os.listdir(self.__reservation_dir)
        for digest in files:
            yield digest, self.__get_data(digest), os.path.join(self.__reservation_dir, digest)


    def join_subranges(self):
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


    def get_subranges(self):
        if self.__child_ranges.size():
            ranges = self.__child_ranges.copy()
            return ranges[self.__ret_range_i], ranges[int(not self.__ret_range_i)]

        return None


    def move_to_trash(self):
        if not os.path.exists(self.__trash_dir):
            os.mkdir(self.__trash_dir)

        self._block_range()
        self._wait_write_buffers()
        files = os.listdir(self.__range_dir)
        for digest in files:
            file_path = os.path.join(self.__range_dir, digest)
            dest = os.path.join(self.__trash_dir, digest)
            if os.path.exists(dest):
                os.remove(dest)

            shutil.move(file_path, dest)

        self._destroy()

    def restore_from_trash(self):
        if not os.path.exists(self.__trash_dir):
            return

        files = os.listdir(self.__trash_dir)
        perc_part = len(files)/10

        logger.info('Restore trash to range...')

        for cnt, digest in enumerate(files):
            if perc_part and (cnt) % perc_part == 0:
                logger.info('Restore progress: %i'%(cnt/perc_part) + '0%...')

            if self._in_range(digest):
                self._move_from(os.path.join(self.__trash_dir, digest), rewrite=False)

        logger.info('Range is restored from trash!')


    def clear_trash(self):
        if not os.path.exists(self.__trash_dir):
            return
        shutil.rmtree(self.__trash_dir)


    def get_range_size(self):
        return sum([os.stat(os.path.join(self.__range_dir, f)).st_size for f in os.listdir(self.__range_dir)])

    def get_free_size(self):
        trash_size = sum([os.stat(os.path.join(self.__trash_dir, f)).st_size for f in os.listdir(self.__trash_dir)])

        stat = os.statvfs(self.__range_dir)
        free_space = stat.f_bsize * stat.f_bavail
        return free_space + trash_size

    def get_free_size_percents(self):
        return (self.get_range_size() * 100.) / self.get_free_size()

