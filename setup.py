import os
import sys
from subprocess import Popen, PIPE
from setuptools import setup, find_packages
from setup_routines import *

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

if __name__ == '__main__':
    prepare_install('/opt/blik/fabnet', '/opt/blik/fabnet/fabnet_package_files.lst')

    setup(
        name = "fabnet-core",
        version = get_cur_ver(),
        author = "Fabregas",
        author_email = "kksstt@gmail.com",
        description = ("Fabnet network core."),
        license = "CC BY-NC",
        url = "https://github.com/fabregas/fabnet/wiki",
        packages= find_packages('.'),
        package_dir={'fabnet': 'fabnet'},
        scripts=get_all('./fabnet/bin'),
        long_description=read('README.md'),
    )
    setup_user()
    update_user_profile()
