import boto
from time import sleep
import time

def logger(msg):
    print (msg)

class AWSControl:

    def __init__(self, cluster_name = "", log_func = logger):
        self.reservations = list()
        self.cluster_name = cluster_name
        self.instance_count = 0
        self.node_set = dict()
        self.logger = log_func
        self.keys = dict()

    def _enumerate_instances(self, create_tags = True):

        for region, inst in self._get_instances():
            node_name = self.cluster_name+str(self.instance_count).zfill(3)
            self.instance_count += 1
            ec2conn = region.connect()
            if create_tags:
                ec2conn.create_tags([inst.id], {'Name': node_name, 'group': self.cluster_name})
            inst.update()
            if not region.name in self.node_set.keys() or not self.node_set[region.name]: self.node_set[region.name] = list()
            self.node_set[region.name].append({node_name: inst.public_dns_name})

    def _gen_keypair(self, region, name):
        ec2conn = region.connect()
        try:
            keypair = ec2conn.get_key_pair(name)
            if not keypair:
                keypair = ec2conn.create_key_pair(name)
            self.keys[region.name] = {'name': keypair.name, 'key': keypair}
        except IOError:
            raise

    def _del_keypair(self, region):
        ec2conn = region.connect()
        if ec2conn.get_key_pair(self.keys[region.name]['name']):
            ec2conn.delete_key_pair(self.keys[region.name]['name'])

    def _get_instances(self):
        for region, reservation in self.reservations:
            ec2conn = region.connect()
            for inst in reservation.instances:
                inst.update()
                yield [region, inst]

    def _print_instances(self):

        self.logger("ID\tZone\t\t\tHostname\tName")
        for region, inst in self._get_instances():
            inst.update()
            self.logger("%s\t%s\t%s\t%s" % (inst.id, region.name, inst.public_dns_name, inst.tags['Name']))

    def create(self, ami_image_id, inst_per_region, only_regions = None, inst_type = 't1.micro', wait_running = False, security_groups = None):

        ec2conn = boto.connect_ec2()
        regions = boto.ec2.regions()
        if only_regions:
            regions = [region for region in regions if region.name in only_regions]
        else:
            regions = [regions[0]] # just take first region from list

        self.logger("Will work in %s regions" % (",".join(map(lambda x:x.name, regions))))
        for region in regions:
            ec2conn = region.connect()
            image = ec2conn.get_all_images(filters = {'name': ami_image_id})[0]
            self.logger("Starting %d instance(s) of %s type in region %s" % (inst_per_region,\
            inst_type, region.name))

            self._gen_keypair(region, self.cluster_name)

            self.reservations.append([region,image.run(inst_per_region, inst_per_region,instance_type = inst_type, security_groups = security_groups, key_name = self.keys[region.name]['name'])])

        if wait_running:
            self.logger("Waiting for instances .")
            inst_states = ['pending']
            while inst_states.count('running') != len(inst_states):
                inst_states = []
                time.sleep(10)
                for region, reservations in self.reservations:
                    ec2conn = region.connect()
                    for inst in reservations.instances:
                        inst.update()
                        inst_states.append(inst.state)
                self.logger(".")
            self.logger('')
        self._enumerate_instances()
        self._print_instances()

    def deploy(self):
        print self.node_set
        self.logger("Getting instances status")
        for region, reservation in self.reservations:
            ec2conn = region.connect()
            for inst in reservation.instances:
                inst.update()
                self.logger("Instance %s status %s" % (inst.id, inst.state))
    def restore(self):
        if not self.reservations:
            ec2conn = boto.connect_ec2()
            regions = boto.ec2.regions()
            for region in regions:
                ec2conn = region.connect()
                for res in ec2conn.get_all_instances():
                    for inst in res.instances:
                        inst.update()
                        if 'group' in inst.tags and inst.tags['group'] == self.cluster_name and inst.state != 'terminated':
                            self.logger('Found orphan %s' % (inst.tags['Name']))
                            self.reservations.append([region, res])
                            self.keys[region.name] = {'name': inst.key_name, 'key': ec2conn.get_key_pair(inst.key_name)}
                            break
        self._enumerate_instances(create_tags = False)

    def teardown(self):
        for region, reservation in self.reservations:
            ec2conn = region.connect()
            print self.reservations
            for inst in reservation.instances:
                inst.terminate()
                self.logger("Terminating %s instance" % (inst.id))
            self._del_keypair(region)

if __name__ == "__main__":
    fabnet = AWSControl(cluster_name = "depl")
#    fabnet.create('amzn-ami-pv-2012.09.0.x86_64-ebs', 3, only_regions = ['eu-west-1'], wait_running = True, security_groups = ['deptest'])
#    print "Continue to terminate ? y/n"
#    choice = raw_input().lower()
#    fabnet.deploy()
    fabnet.restore()
    print fabnet.reservations, fabnet.node_set
#    fabnet.teardown()
