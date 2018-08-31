#!/usr/bin/python3
import zc.lockfile #pip3 install zc.lockfile
import sys
id = sys.argv[1]

try:
    lock = zc.lockfile.LockFile('lock'+id, content_template='{pid}')
    print(-1)
except zc.lockfile.LockError:
    f = open('lock'+id,'r')
    print(f.readline())
    f.close()
