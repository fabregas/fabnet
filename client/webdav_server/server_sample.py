import os
import shutil
import time
import subprocess
import signal

from wsgidav.version import __version__
from wsgidav.wsgidav_app import DEFAULT_CONFIG, WsgiDAVApp
from wsgidav.fs_dav_provider import FilesystemProvider

from fabnet_dav_provider import FabnetProvider

from cherrypy import wsgiserver, __version__ as cp_version


class SecurityProviderMock:
    def get_user_id(self):
        return 'this is test USER ID string'

    def get_network_key(self):
        pass

    def encrypt(self, data):
        return data

    def decrypt(self, data):
        return data

##############################


from client.nibbler import Nibbler

n_node = 'init-fabnet'
i = 1987
address = '127.0.0.1:%s'%i

home = '/tmp/node_%s'%i
if os.path.exists(home):
    shutil.rmtree(home)
os.mkdir(home)

args = ['/usr/bin/python', './fabnet/bin/fabnet-node', address, n_node, '%.02i'%i, home]
p = subprocess.Popen(args)
time.sleep(1.5)

nibbler = Nibbler('127.0.0.1', SecurityProviderMock())
nibbler.register_user()

##############################


security_provider = SecurityProviderMock()

provider = FabnetProvider('127.0.0.1', security_provider)
#provider = FilesystemProvider('/tmp')

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
    print("Runing %s, listening on %s://%s:%s" % (version, 'http', '0.0.0.0', 8008))
server = wsgiserver.CherryPyWSGIServer(('0.0.0.0', 8008), app,)

try:
    server.start()
except Exception, err:
    print 'error: %s'%err
    p.send_signal(signal.SIGINT)
    p.wait()
except KeyboardInterrupt, err:
    server.stop()
    p.send_signal(signal.SIGINT)
    p.wait()

