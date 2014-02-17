import os
import sys
from subprocess import Popen, PIPE
from setuptools import setup, find_packages

USER_NAME = 'fabnet'

GENTOO = 1
RHEL = 2
DEBIAN = 3

os.environ['PYTHONPATH'] = '/opt/blik/fabnet/packages'

def get_all(path):
    items = os.listdir(path)
    return [os.path.join(path, item) for item in items]


def get_cur_ver():
    if not os.path.exists('VERSION'):
        raise Exception('No VERSION file found!')


    ver = open('VERSION').read()
    return ver.strip()


def clear_empty_dir(path):
    lst = os.listdir(path)
    for item in lst:
        n_path = os.path.join(path, item)
        if os.path.isdir(n_path):
            clear_empty_dir(n_path)

    lst = os.listdir(path)
    if not lst:
        os.rmdir(path)
        print ' -> removed empty dir %s'%path
        return


def prepare_install(path, lst_file_path):
    if not os.path.exists(lst_file_path):
        return

    with open(lst_file_path) as fd:
        for line in fd:
            line = line.strip()
            if not line:
                continue
            try:
                os.remove(line)
                print ' -> removed old %s'%line
            except OSError, err:
                print 'Warning! file %s does not removed. %s'%(line, err)

    clear_empty_dir(path)


def setup_user():
    data = open('/etc/passwd').read()
    if USER_NAME in data:
        print('User %s is already exists in system'%USER_NAME)
    else:
        ret = os.system('useradd -m %s'%USER_NAME)
        if ret:
            print('ERROR! Can not create user %s'%USER_NAME)
            sys.exit(1)

    data = open('/etc/group').read()
    if USER_NAME in data:
        print('Group %s is already exists in system'%USER_NAME)
    else:
        ret = os.system('groupadd %s'%USER_NAME)
        if ret:
            print('ERROR! Can not create user group %s'%USER_NAME)
            sys.exit(1)

    os.system('usermod -a -G %s %s'%(USER_NAME, USER_NAME))
    os.system('usermod -a -G wheel %s'%(USER_NAME,))

    

def update_user_profile():
    profile_path = os.path.join('/home/%s'%USER_NAME, '.bashrc')
    ins_data = 'export PYTHONPATH=$PYTHONPATH:/opt/blik/fabnet/packages'
    if os.path.exists(profile_path):
        data = open(profile_path).read()
        if ins_data in data:
            print 'PYPTHONPATH is already installed correctly'
            return
    open(profile_path, 'a').write('\n%s\n'%ins_data)
    os.system('chown %s:%s %s'%(USER_NAME, USER_NAME, profile_path))


def get_linux_distr():
    if os.path.exists('/usr/bin/emerge'):
        return GENTOO, 'sudo emerge --update %s'
    if os.path.exists('/usr/bin/yum'):
        return RHEL, 'sudo yum update -y %s'
    if os.path.exists('/usr/bin/apt-get'):
        return DEBIAN, 'sudo apt-get install %s'
    raise Exception('Unsupported linux distribution detected!')

def check_deps(deps):
    distr, cmd = get_linux_distr()
    packages = deps.get(distr, [])
    if not packages:
        return
    packages_str = ' '.join(packages)
    print 'Install dependencies: %s'%packages_str
    ret = os.system(cmd % packages_str)
    if ret:
        raise Exception('ERROR! Failed installation!')


def install_submodule(submodule_path):
    ret = os.system('sudo easy_install %s' % submodule_path)
    if ret:
        raise Exception('ERROR! Failed submodule installation!')

