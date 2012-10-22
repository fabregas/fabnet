#!/usr/bin/python
import os
import sys
import signal

client_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
third_party = os.path.join(client_dir, 'client/third-party')

sys.path.insert(0, third_party)
sys.path.insert(0, client_dir)

from client.webdav_server.server import make_server

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print 'usage: %s <server ipaddr> <server port> <fabnet hostname>'%sys.argv[0]
        sys.exit(1)

    host, port, fabnet_host = sys.argv[1:]
    server = make_server(host, int(port), fabnet_host)

    def stop(s, p):
        global server
        try:
            server.stop()
            server.provider.stop()
        except Exception, err:
            #FIXME: logger.error('Stopping client error: %s'%err)
            print 'ERROR: %s'%err

    signal.signal(signal.SIGINT, stop)

    server.start()

