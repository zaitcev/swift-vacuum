#
# This only works on containers for now.
# XXX ... and does not check if the db is a container
#
#  python swift_vacuum.py \
#     /srv/node/a1/containers/195921/a75/bf5xxxa75/bf5xxxa75.db
#

from __future__ import print_function

import os
import pwd
import sys
import sqlite3

# from swift.container.backend import ContainerBroker
from swift.common.utils import lock_parent_directory

def set_swift_id():
    # If we run as root, we are working with a production database and
    # probably want to operate as swift. If we do not run as root,
    # we may be testing, so leave the identity alone.
    if os.geteuid() == 0:
        user = pwd.getpwnam('swift')
        os.setgid(user[3])
        os.setuid(user[2])

class Usage(Exception):
    pass

def main(argv):
    try:
        if len(argv) != 2:
            raise Usage
    except Usage:
        print("Usage: swift-vacuum db_file", file=sys.stderr)
        return 1
    db_file = argv[1]

    set_swift_id()

    # brokerclass = ContainerBroker
    # broker = brokerclass(db_file, pending_timeout=30)

    with lock_parent_directory(db_file):
        # P3
        print('vacuuming %s' % db_file)

        conn = sqlite3.connect(db_file, timeout=0)
        conn.execute('VACUUM;')
        conn.commit()
        conn.close()

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
