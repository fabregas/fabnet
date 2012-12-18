import os
import sys
from fabnet.utils.upgrade_script_base import BaseFabnetUpgradeScript

class InstallPythonDaemon(BaseFabnetUpgradeScript):
    def upgrade_rpm(self, osver, is64bit):
        self.yum_install('python-daemon')

    def upgrade_ebuild(self, osver, is64bit):
        self.emerge_install('python-daemon')


if __name__ == '__main__':
    sys.exit(InstallPythonDaemon().run())
