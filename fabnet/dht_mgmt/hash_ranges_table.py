import threading
import pickle
import copy


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

    def length(self):
        return self.__length


class HashRangesTable:
    def __init__(self):
        self.__ranges = []
        self.__lock = threading.RLock()

    def append(self, start, end, node_addr):
        self.__lock.acquire()
        try:
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

            self.__sorted_insert(HashRange(start, end, node_addr))
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


    def remove(self, ex_hash):
        self.__lock.acquire()
        try:
            r_obj = self.find(ex_hash)
            if not r_obj:
                raise RangeException('Range does not found for hash %s'%ex_hash)

            del self.__ranges[self.__ranges.index(r_obj)]
        finally:
            self.__lock.release()

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
            return pickle.dumps(self.__ranges)
        finally:
            self.__lock.release()

    def load(self, ranges_dump):
        self.__lock.acquire()
        try:
            self.__ranges = pickle.loads(ranges_dump)
        finally:
            self.__lock.release()

    def find(self, find_value):
        self.__lock.acquire()
        try:
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
                return self.__ranges[cur_n]
        finally:
            self.__lock.release()

    def iter_table(self):
        self.__lock.acquire()
        try:
            for range_obj in self.__ranges:
                yield range_obj
        finally:
            self.__lock.release()



