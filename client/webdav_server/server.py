import os
import shutil
import time
import signal
import sys

third_party = os.path.abspath(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../third-party'))
sys.path.insert(0, third_party)

from wsgidav.version import __version__
from wsgidav.wsgidav_app import DEFAULT_CONFIG, WsgiDAVApp
from wsgidav.fs_dav_provider import FilesystemProvider

from fabnet_dav_provider import FabnetProvider

from cherrypy import wsgiserver, __version__ as cp_version


class SecurityProviderMock:
    def get_user_id(self):
        return 'this is test USER ID string'

    def get_client_cert(self):
        return 'fake cert'

    def get_client_cert_key(self):
        return

    def encrypt(self, data):
        return data

    def decrypt(self, data):
        return data




def make_server(host, port, fabnet_host):
    security_provider = SecurityProviderMock()

    provider = FabnetProvider(fabnet_host, security_provider)

    config = DEFAULT_CONFIG.copy()
    config.update({
        "provider_mapping": {"/": provider},
        "user_mapping": {},
        "verbose": 1,
        "enable_loggers": [],
        "propsmanager": True,      # True: use property_manager.PropertyManager                    
        "locksmanager": True,      # True: use lock_manager.LockManager                   
        "domaincontroller": None,  # None: domain_controller.WsgiDAVDomainController(user_mapping)
        })
    app = WsgiDAVApp(config)

    version = "WsgiDAV/%s %s" % (__version__, wsgiserver.CherryPyWSGIServer.version)
    wsgiserver.CherryPyWSGIServer.version = version
    if config["verbose"] >= 1:
        print("Runing %s, listening on %s://%s:%s" % (version, 'http', host, port))

    server = wsgiserver.CherryPyWSGIServer((host, port), app,)
    server.provider = provider
    return server

