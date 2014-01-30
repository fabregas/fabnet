import os
import sys
from subprocess import Popen, PIPE
from setuptools import setup, find_packages

USER_NAME = 'fabnet'
os.environ['PYTHONPATH'] = '/opt/blik/fabnet/packages'

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def get_all(path):
    items = os.listdir(path)
    return [os.path.join(path, item) for item in items]


def get_cur_ver():
    p = Popen(['git', 'describe', '--always', '--tag'], stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        print ('ERROR! git describe failed: %s'%err)
        sys.exit(1)
    return out.strip()


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

if __name__ == '__main__':
    setup(
        name = "fabnet-core",
        version = get_cur_ver(),
        author = "Fabregas",
        author_email = "kksstt@gmail.com",
        description = ("Fabnet network core."),
        license = "CC BY-NC",
        url = "https://github.com/fabregas/fabnet/wiki",
        packages= find_packages('.'),
        #package_dir={'fabnet': 'fabnet_core/fabnet', 'fabnet.utils': 'fabnet_core/fabnet/utils'},
        package_dir={'fabnet': 'fabnet'},
        scripts=get_all('./fabnet/bin'),
        long_description=read('README.md'),
    )
    setup_user()
    update_user_profile()
