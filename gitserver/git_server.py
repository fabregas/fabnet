
import os
from wsgiserver import CherryPyWSGIServer
from git_http_backend import assemble_WSGI_git_app

GIT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.git'))
app = assemble_WSGI_git_app(content_path = GIT_DIR, uri_marker='fabnet-node-repo.git')

httpd = CherryPyWSGIServer(('0.0.0.0',8080), app)

try:
    httpd.start()
except KeyboardInterrupt:
    pass
finally:
    httpd.stop()
