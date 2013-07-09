import os
import sys
from fabnet.utils.upgrade_script_base import BaseFabnetUpgradeScript

class InstallPythonYaml(BaseFabnetUpgradeScript):
    def upgrade_rpm(self, osver, is64bit):
        self.yum_install('python-yaml')

    def upgrade_ebuild(self, osver, is64bit):
        self.emerge_install('pyyaml')

    def upgrade_deb(self, osver, is64bit):
        self.aptget_install('python-yaml')


if __name__ == '__main__':
    sys.exit(InstallPythonYaml().run())
