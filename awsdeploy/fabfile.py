from aws_control import AWSControl
from fabric.api import settings, hosts, env, run, execute, task, sudo, prefix, cd, roles
from fabric.colors import red, green
import os.path
from os import remove
import pickle
import time

TMPDIR="/tmp" #FIXME: add here mkstemp

class FabnetDeploy(AWSControl):

    def _get_hostlist(self):
        hostlist = list()
        for region in self.node_set:
            for node in self.node_set[region]:
                for h, n in node.items():
                    hostlist.append(n)
        return hostlist

@task
def configure():
    fbn.create('amzn-ami-pv-2012.09.0.x86_64-ebs', 3, only_regions = ['eu-west-1'], wait_running = True, security_groups = ['deptest'])
    env.hosts = fbn._get_hostlist()
    fake_hosts = env.hosts
    if not os.path.isfile('/tmp/'+fbn.keys['eu-west-1']['name']+'.pem'):
        fbn.keys['eu-west-1']['key'].save("/tmp")
    env.roledefs = {'first': [env.hosts[0]], 'next': env.hosts[1:]}


# main
fbn = FabnetDeploy(cluster_name = 'depl')
metaenv = env
metaenv.connection_attempts = 20
metaenv.timeout = 30
metaenv.user = "ec2-user"
metaenv.parallel = False
metaenv.key_filename = TMPDIR+"/"+fbn.cluster_name+".pem"
metaenv.skip_bad_hosts = True
fake_hosts = []
first_node = ''

@task
@roles('first','next')
def install():
    if sudo('yum --assumeyes install git'):
        run("rm -rf ~/fabnet_node ~/fabnet_node_home")
        run('wget http://repo.idepositbox.com:8080/repos/install_fabnet_node.sh')
        run('chmod +x ./install_fabnet_node.sh')
        run('./install_fabnet_node.sh')
@task
@hosts('')
def start():
    with settings(user = 'ec2-user'):
        execute(run_first_node)
        execute(run_next_nodes)
    for _ in xrange(50): print(green("=")),
    print
    for node in metaenv.hosts:
        print (green(node))
    print (red("First node " + env.first_node))

@task
@roles('first')
def run_first_node():
    print "ENVTASK", env.hosts
    env.first_node = env.hosts[0]
    with prefix('export FABNET_NODE_HOST='+str(env.host)):
        run('cd ~/fabnet_node;nohup ./fabnet/bin/node-daemon start init-fabnet depl000 DHT')

@task
@roles('next')
def run_next_nodes():
    with prefix('export FABNET_NODE_HOST='+str(env.host)):
        run('cd ~/fabnet_node; nohup ./fabnet/bin/node-daemon start '+env.first_node+' depl001 DHT')

@task
@hosts('')
def restore():
    env = metaenv
    fbn.restore()
    hosts = fbn._get_hostlist()
    env.first_node = hosts[0]
    env.hosts = hosts
    env.user = 'ec2-user'
    env.roledefs = {'first': [hosts[0]], 'next': hosts[1:]}

@task
def kill_after_fail():
    fbn.restore()
    metaenv.hosts = fbn._get_hostlist()
    env = metaenv
    execute(teardown)

@task
@hosts('')
def teardown():
    fbn.teardown()
    if os.path.isfile(metaenv.key_filename):
        remove(metaenv.key_filename)

