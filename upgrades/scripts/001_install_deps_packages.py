import os
import sys
from fabnet.utils.upgrade_script_base import BaseFabnetUpgradeScript

class InstallDeps(BaseFabnetUpgradeScript):
    def upgrade_rpm(self, osver, is64bit):
        metarpm_path = os.path.join(self._get_data_dir(), 'fabnet-meta-0.1-0.noarch.rpm')
        ret = os.system('sudo rpm -ivh %s'%metarpm_path)

        self.yum_install('m2crypto')

    def upgrade_ebuild(self, osver, is64bit):
        self.emerge_install('m2crypto')

    def upgrade_deb(self, osver, is64bit):
        self.aptget_install('python-m2crypto')



if __name__ == '__main__':
    sys.exit(InstallDeps().run())
