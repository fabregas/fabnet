#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.agents_manager
@author Konstantin Andrusenko
@date November 16, 2012
"""
import threading
import traceback
from fabnet.utils.logger import logger
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.threads_manager import ThreadsManager
from fabnet.core.constants import RC_OK, RC_DONT_STARTED, STOP_THREAD_EVENT, \
                                MIN_WORKERS_COUNT, MAX_WORKERS_COUNT

class FriAgent(threading.Thread):
    def __init__(self, queue, ret_queue, keystorage):
        threading.Thread.__init__(self)

        self.queue = queue
        self.ret_queue = ret_queue
        self.key_storage = keystorage

        if keystorage:
            cert = keystorage.get_node_cert()
            ckey = keystorage.get_node_cert_key()
        else:
            cert = ckey = None
        self.fri_client = FriClient(bool(cert), cert, ckey)
        self.__busy_flag = threading.Event()

    def is_busy(self):
        return self.__busy_flag.is_set()

    def run(self):
        logger.info('agent is started!')

        while True:
            try:
                self.__busy_flag.clear()

                item = self.queue.get()

                if item == STOP_THREAD_EVENT:
                    logger.info('agent is stopped!')
                    break

                self.__busy_flag.set()

                if len(item) != 4:
                    raise Exception('Expected (<address>,<packet>,<event>,<sync_callback>), but "%s" occured'%item)

                address, packet, sync_event, async_callback = item

                if sync_event is not None:
                    resp = self.fri_client.call_sync(address, packet)
                    self.ret_queue.put(packet.message_id, resp)
                    sync_event.set()
                else:
                    rcode, rmsg = self.fri_client.call(address, packet)
                    if rcode != RC_OK:
                        logger.error("Can't call async operation %s on %s. Details: %s"%\
                            (getattr(packet, 'method', 'callback'), address, rmsg))
                        logger.debug('Failed packet: %s'%packet)
                        ret_packet = FabnetPacketResponse(message_id=packet.message_id, \
                                       from_node=address, ret_code=RC_DONT_STARTED, ret_message=rmsg)
                        async_callback(ret_packet)
            except Exception, err:
                ret_message = 'run() error: %s' % err
                logger.write = logger.debug
                traceback.print_exc(file=logger)
                logger.error(ret_message)
            finally:
                self.queue.task_done()


class FriAgentsManager(ThreadsManager):
    def __init__(self, queue, ret_queue, keystorage, min_count=MIN_WORKERS_COUNT, \
                    max_count=MAX_WORKERS_COUNT, prefix='unknown'):
        ThreadsManager.__init__(self, queue, min_count, max_count, prefix, \
                                    FriAgent, (queue, ret_queue, keystorage))

