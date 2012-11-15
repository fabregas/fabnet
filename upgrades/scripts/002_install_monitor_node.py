import os
import sys
from fabnet.utils.upgrade_script_base import BaseFabnetUpgradeScript

class InstallMonitor(BaseFabnetUpgradeScript):
    def get_target_node_roles(self):
        return ['Monitor']

    def print_notice(self):
        print '-'*80
        print 'WARNING! You should configure postgresql server and start it on localhost'
        print '-'*80

    def upgrade_rpm(self, osver, is64bit):
        self.yum_install('postgresql-server')
        self.yum_install('python-psycopg2')
        self.print_notice()

    def upgrade_ebuild(self, osver, is64bit):
        self.emerge_install('postgresql-server')
        self.emerge_install('psycopg')
        self.print_notice()


if __name__ == '__main__':
    sys.exit(InstallMonitor().run())
