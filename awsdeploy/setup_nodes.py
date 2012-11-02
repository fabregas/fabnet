import boto
from time import sleep
import time

class InstancesControl:
    def __init__(self):
        pass
    def create(self, instances_list = None):
        pass
    def deploy(self):
        pass
    def teardown(self):
        pass

    def _start_instance(self, instance, wait = True):
        instance.start()
        if wait and instance.state not in ('running'):
            for i in xrange(10):
                instance.update()
                if instance.state == 'running':
                    return True
                sleep(10)
        return False


class FabnetDeploy(InstancesControl):

    def __init__(self, cluster_name = ""):
        self.reservations = list()
        self.cluster_name = cluster_name
        self.instance_count = 0

    def _enumerate_instances(self):

        for region, inst in self._get_instances():
            node_name = self.cluster_name+str(self.instance_count).zfill(3)
            self.instance_count += 1
            ec2conn = region.connect()
            ec2conn.create_tags([inst.id], {'Name': node_name, 'group': self.cluster_name})

    def _get_instances(self):
        for region, reservation in self.reservations:
            ec2conn = region.connect()
            for inst in reservation.instances:
                inst.update()
                yield [region, inst]

    def _print_instances(self):

        print "ID\tZone\t\t\tHostname\tName"
        for region, inst in self._get_instances():
            print "%s\t%s\t%s\t%s" % (inst.id, region.name, inst.public_dns_name, inst.tags['Name'])

    def create(self, ami_image_id, inst_per_region, only_regions = None, inst_type = 't1.micro', wait_running = False, security_groups = None, keys = None):

        ec2conn = boto.connect_ec2()
        regions = boto.ec2.regions()
        if only_regions:
            regions = [region for region in regions if region.name in only_regions]
        else:
            regions = [regions[0]] # just take first region from list

        print "Will work in %s regions" % (",".join(map(lambda x:x.name, regions)))
        for region in regions:
            ec2conn = region.connect()
            image = ec2conn.get_all_images(filters = {'name': ami_image_id})[0]
            print "Starting %d instance(s) of %s type in region %s" % (inst_per_region,\
            inst_type, region.name)

            self.reservations.append([ region,image.run(inst_per_region, inst_per_region,\
            instance_type = inst_type, security_groups = security_groups, key_name = keys)])

        self._enumerate_instances()
        self._print_instances()
        if wait_running:
            print "Waiting for instances .",
            inst_states = ['pending']
            while inst_states.count('running') != len(inst_states):
                inst_states = []
                time.sleep(10)
                for region, reservations in self.reservations:
                    ec2conn = region.connect()
                    for inst in reservations.instances:
                        inst.update()
                        inst_states.append(inst.state)
                print ".",
            print

    def deploy(self):
        print "Getting instances status"
        for region, reservation in self.reservations:
            ec2conn = region.connect()
            for inst in reservation.instances:
                inst.update()
                print "Instance %s status %s" % (inst.id, inst.state)


    def teardown(self):
        for region, reservation in self.reservations:
            ec2conn = region.connect()

            for inst in reservation.instances:
                inst.terminate()
                print "Terminating %s instance" % (inst.id)


if __name__ == "__main__":
    fabnet = FabnetDeploy(cluster_name = "deptest")
    fabnet.create('amzn-ami-pv-2012.09.0.x86_64-ebs', 3, only_regions = ['eu-west-1'], wait_running = True, security_groups = ['deptest'], keys = 'deptest')
    print "Continue to terminate ? y/n"
    choice = raw_input().lower()
    fabnet.deploy()
    fabnet.teardown()
