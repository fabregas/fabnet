#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.statistic
@author Konstantin Andrusenko
@date January 7, 2013

This module contains the classes implementation for working with statistic

                    Statistic objects paths:
     StatisticCollector -> StatMap -> operator -> Statistic
     OSProcessesStatisticCollector -> operator -> Statistic
"""

import copy
import time
import threading

from fabnet.utils.logger import logger

class StatAtom:
    def __init__(self, value, cnt=1):
        self.__cnt = cnt
        self.__value = value

    def update(self, value, accumulate_cnt=None):
        if accumulate_cnt is None:
            self.__value = value
            return

        self.__value += value
        self.__cnt += accumulate_cnt

    def __add__(self, other):
        new = copy.copy(other)
        new.update(self.__value, self.__cnt)
        return new

    def value(self):
        if not self.__cnt:
            return self.__value
        return self.__value / float(self.__cnt)

    def dump(self):
        if not self.__cnt:
            return self.__value
        return (self.__cnt, self.__value)

    def reset(self):
        self.__cnt = 0
        self.__value = 0


class StatMap:
    def __init__(self):
        self.__stat_map = {}
        self.__lock = threading.Lock()

    def update(self, key, value, cnt=1):
        self.__lock.acquire()
        try:
            cval = self.__stat_map.get(key, None)
            if not cval:
                cval = StatAtom(value, cnt)
                self.__stat_map[key] = cval
            else:
                cval.update(value, cnt)
        finally:
            self.__lock.release()

    def set(self, key, value):
        self.__lock.acquire()
        try:
            self.__stat_map[key] = StatAtom(value)
        finally:
            self.__lock.release()

    def dump(self):
        self.__lock.acquire()
        try:
            ret_map = {}
            for key, value in self.__stat_map.items():
                ret_map[key] = value.dump()

            self.__stat_map = {}
            return ret_map
        finally:
            self.__lock.release()


class Statistic:
    def __init__(self):
        self.__stat = {}
        self.__lock = threading.Lock()

    def update(self, stat_obj, stat_owner, stat):
        self.__lock.acquire()
        try:
            stat_obj_d = self.__stat.get(stat_obj, {})
            if not stat_obj_d:
                self.__stat[stat_obj] = stat_obj_d

            stat_owner_d = stat_obj_d.get(stat_owner, {})
            if not stat_owner_d:
                stat_obj_d[stat_owner] = stat_owner_d

            for key, value in stat.items():
                if type(value) in (list, tuple):
                    cnt, value = value
                else:
                    cnt = 1
                cval = stat_owner_d.get(key, None)
                if not cval:
                    cval = StatAtom(value, cnt)
                    stat_owner_d[key] = cval
                else:
                    cval.update(value, cnt)
        finally:
            self.__lock.release()

    def reset(self):
        self.__lock.acquire()
        try:
            for stat_obj_d in self.__stat.values():
                for stat_owner_d in stat_obj_d.values():
                    for value in stat_owner_d.values():
                        value.reset()
        finally:
            self.__lock.release()

    def dump(self):
        self.__lock.acquire()
        try:
            ret_stat = {}
            for stat_obj, stat_obj_d in self.__stat.items():
                raw_stat = {}
                for stat_owner, stat_owner_d in stat_obj_d.items():
                    for key, value in stat_owner_d.items():
                        if key in raw_stat:
                            raw_stat[key] += value
                        else:
                            raw_stat[key] = value

                for key, value in raw_stat.items():
                    raw_stat[key] = value.value()

                ret_stat[stat_obj] = raw_stat
            return ret_stat
        finally:
            self.__lock.release()


class StatisticCollector(threading.Thread):
    def __init__(self, operator_client, stat_obj, stat_owner, stat_map, timeout):
        threading.Thread.__init__(self)

        self.operator_cl = operator_client
        self.stat_map = stat_map
        self.stat_obj = stat_obj
        self.stat_owner = stat_owner
        self.timeout = int(timeout)
        self.setName('%s-statcol-%s'%(stat_obj, stat_owner))
        self.stop_flag = threading.Event()

    def run(self):
        logger.debug('StatisticCollector is started!')
        last_stat = None
        while True:
            try:
                for i in xrange(self.timeout):
                    time.sleep(1)
                    if self.stop_flag.is_set():
                        break

                if self.stop_flag.is_set():
                    break

                #send statistic to operator...
                stat = self.stat_map.dump()
                if stat == last_stat:
                    continue
                last_stat = stat
                self.operator_cl.update_statistic(self.stat_obj, self.stat_owner, stat)
            except Exception, err:
                import traceback
                logger.write = logger.debug
                traceback.print_exc(file=logger)
                logger.error(str(err))
        logger.debug('StatisticCollector is stopped!')

    def stop(self):
        if not self.stop_flag.is_set():
            self.stop_flag.set()
            self.join()



class OSProcessesStatisticCollector(threading.Thread):
    def __init__(self, operator_client, pid_list, workers_mgr_list, timeout):
        threading.Thread.__init__(self)

        self.operator_cl = operator_client
        self.timeout = int(timeout)
        self.pid_list = pid_list
        self.workers_manager_list = workers_mgr_list
        self.setName('osprocesses-statcol')
        self.stop_flag = threading.Event()

    def run(self):
        logger.debug('OSProcessesStatisticCollector is started!')
        while True:
            try:
                for i in xrange(self.timeout):
                    time.sleep(1)
                    if self.stop_flag.is_set():
                        break

                if self.stop_flag.is_set():
                    break

                for pid_group_name, pid in self.pid_list:
                    p_stat = self.get_process_stat(pid)
                    self.operator_cl.update_statistic('%sProcStat'%pid_group_name, pid, p_stat)

                for workers_manager in self.workers_manager_list:
                    w_act, w_busy = workers_manager.get_workers_stat()
                    w_count = w_act + w_busy

                    self.operator_cl.update_statistic('%sWMStat'%workers_manager.get_workers_name(), \
                                            'WM', {'workers': w_count, 'busy': w_busy})
            except Exception, err:
                import traceback
                logger.write = logger.debug
                traceback.print_exc(file=logger)
                logger.error(str(err))
        logger.debug('OSProcessesStatisticCollector is stopped!')

    @classmethod
    def get_process_stat(cls, pid):
        rss = threads = ''
        lines = open('/proc/%i/status'%pid,'r').readlines()
        for line in lines:
            (param, value) = line.split()[:2]
            if param.startswith('VmRSS'):
                rss = value.strip()
            elif param.startswith('Threads'):
                threads = value.strip()

        procinfo = {}
        procinfo['memory'] = int(rss)
        procinfo['threads'] = int(threads)
        return procinfo

    def stop(self):
        if not self.stop_flag.is_set():
            self.stop_flag.set()
            self.join()




"""

stat = Statistic()
stat.update('fabstat', 'node01', {'temp': (10, 4336.43), 'temp2': [12, 333], 'age': 25})
print stat.dump()
stat.update('fabstat', 'node02', {'temp': (10, 4676.43), 'temp2': [15, 433], 'age': 25})
print stat.dump()
"""
