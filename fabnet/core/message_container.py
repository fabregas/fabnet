#!/usr/bin/python
"""
Copyright (C) 2011 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.message_container
@author Konstantin Andrusenko
@date August 20, 2012

This module contains the MessageContainer class implementation
"""

import threading
import Queue

class MessageContainer:
    def __init__(self, size):
        self.__ordered_ids = Queue.Queue(size)
        self.__lock = threading.RLock()
        self.__messages = {}

    def put(self, message_id, message):
        self.__lock.acquire()
        try:
            if self.__ordered_ids.full():
                del_id = self.__ordered_ids.get()
                del self.__messages[del_id]

            self.__ordered_ids.put(message_id)

            self.__messages[message_id] = message
        finally:
            self.__lock.release()

    def put_safe(self, message_id, message):
        self.__lock.acquire()
        try:
            s_msg  = self.get(message_id)
            if s_msg is not None:
                return False
            self.put(message_id, message)
            return True
        finally:
            self.__lock.release()

    def get(self, message_id, default=None):
        self.__lock.acquire()
        try:
            return self.__messages.get(message_id, default)
        finally:
            self.__lock.release()


#------------------------------------------------------------------------------

if __name__ == '__main__':
    mc = MessageContainer(2)
    mc.put(1, 'im first')
    mc.put(2, 'im second')
    print '1: ', mc.get(1)
    print '2: ', mc.get(2)

    mc.put(3, 'im third')
    print 'After thrid appended: '
    print '1: ', mc.get(1)
    print '2: ', mc.get(2)
    print '3: ', mc.get(3)

    print 'inserted 3: ', mc.put_safe(3, 'im dond put')
    print 'inserted 4:', mc.put_safe(4, 'im fourth')
    print '4: ', mc.get(4)

