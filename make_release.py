#!/usr/bin/python

import sys
import os

if len(sys.argv) != 2:
    print('Usage: %s <version>'%sys.argv[0])
    sys.exit(1)

open('VERSION', 'w').write(sys.argv[1])
ret = os.system('git add VERSION')
if ret:
    print('ERROR! "git add" failed!')
    sys.exit(1)

ret = os.system("git commit -m 'updated version file (%s)'"%sys.argv[1])

ret = os.system('git tag %s -a'%sys.argv[1])
if ret:
    print('ERROR! "git tag" failed!')
    sys.exit(1)

