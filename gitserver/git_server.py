#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package gitserver
@author Konstantin Andrusenko
@date September 15, 2012

This is HTTP(S) Git server (depends on cherrypy ext module)

For cloning https git repo, client should call:
    GIT_SSL_CERT='<path to PEM cert>' git clone https://<fabnet-node-host>:8080/fabnet-node-repo.git/
"""
import os
import sys
import signal
import socket

from cherrypy.wsgiserver import CherryPyWSGIServer
from cherrypy.wsgiserver.ssl_builtin import BuiltinSSLAdapter as SSLAdapter
from git_http_backend import assemble_WSGI_git_app

GIT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.git'))
URI_MARKER = 'fabnet-node-repo.git'
PORT = 8080

httpd = None

def stop(s, p):
    global httpd
    try:
        if httpd:
            httpd.stop()
        sys.stdout.write('git server is stopped\n')
    except Exception, err:
        sys.stderr.write('stopping server error: %s\b'%err)

def is_server_started():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.settimeout(1)
    try:
        sock.connect(('127.0.0.1', PORT))
        return True
    except Exception, err:
        return False
    finally:
        sock.close()


if __name__ == '__main__':
    if is_server_started():
        print('Git server is already started')
        sys.exit(0)

    certfile = keyfile = None
    if len(sys.argv) > 1:
        certfile = sys.argv[1]
    if len(sys.argv) > 2:
        keyfile = sys.argv[2]

    app = assemble_WSGI_git_app(content_path=GIT_DIR, uri_marker=URI_MARKER)
    httpd = CherryPyWSGIServer(('0.0.0.0', PORT), app)

    if certfile:
        httpd.ssl_adapter = SSLAdapter(certfile, keyfile)

    signal.signal(signal.SIGINT, stop)
    httpd.start()

