#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.upgrade_script_base
@author Konstantin Andrusenko
@date September 16, 2012

This module contains the BaseFabnetUpgradeScript class implementation
"""
import sys
import os
import platform

TP_GENTOO = 'gentoo'
TP_RHEL = 'rhel'
TP_DEBIAN = 'debian'
TP_MACOS = 'macos'
TP_WIN = 'win'


class BaseFabnetUpgradeScript:
    def __init__(self):
        self.__data_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '../../upgrades/data'))

    def _get_data_dir(self):
        return self.__data_dir

    def _get_target_platform(self):
        is64bit = False
        if sys.platform.startswith('linux'):
            sys_release = os.uname()[2]
            if os.uname()[-1] == 'x86_64':
                is64bit == True

            if os.path.exists('/usr/bin/emerge'):
                return TP_GENTOO, sys_release, is64bit
            if os.path.exists('/usr/bin/yum'):
                return TP_RHEL, sys_release, is64bit
            if os.path.exists('/usr/bin/apt-get'):
                return TP_DEBIAN, sys_release, is64bit

            raise Exception('Unsupported linux distribution detected!')

        elif sys.platform == 'darwin':
            sys_release = os.uname()[2]
            if os.uname()[-1] == 'x86_64':
                is64bit == True
            return TP_MACOS, sys_release, is64bit

        elif sys.platform == 'win':
            is64bit = 'PROGRAMFILES(X86)' in os.environ
            return TP_WIN, platform.release(), is64bit
        else:
            raise Exception('Unknown platform: %s'%sys.platform)

    def __run_int(self):
        target_platf, os_release, is64bit = self._get_target_platform()

        if target_platf == TP_WIN:
            self.upgrade_win(os_release, is64bit)
        elif target_platf == TP_MACOS:
            self.upgrade_macos(os_release, is64bit)
        elif target_platf == TP_RHEL:
            self.upgrade_rpm(os_release, is64bit)
        elif target_platf == TP_DEBIAN:
            self.upgrade_deb(os_release, is64bit)
        elif target_platf == TP_GENTOO:
            self.upgrade_ebuild(os_release, is64bit)
        else:
            raise Exception('Unsupported platform: %s'%target_platf)

    def run(self):
        try:
            self.__run_int()
        except Exception, err:
            sys.stderr.write('ERROR: %s\n'%err)

    def yum_install(self, package):
        cmd = 'sudo yum -y install %s'%package
        print(cmd)
        ret = os.system(cmd)
        if ret:
            raise Exception('"%s" failed!'%cmd)

    def emerge_install(self, package):
        cmd = 'sudo emerge -v %s'%package
        print(cmd)
        ret = os.system(cmd)
        if ret:
            raise Exception('"%s" failed!'%cmd)

    def upgrade_win(self, osver, is64bit):
        pass

    def upgrade_macos(self, osver, is64bit):
        pass

    def upgrade_rpm(self, osver, is64bit):
        pass

    def upgrade_deb(self, osver, is64bit):
        pass

    def upgrade_ebuild(self, osver, is64bit):
        pass

