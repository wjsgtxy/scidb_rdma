#!/usr/bin/env python
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2019 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

#Become postgres user and
#Edit /etc/postgresql/8.4/main/pg_hba.conf
#Append the following configuration lines to give access to 10.X.Y.Z/N network:
#host all all 10.X.Y.Z/N md5
# For example: 10.0.20.0/24
#
#Edit /etc/postgresql/8.4/main/postgresql.conf
#Set IP address(es) to listen on; you can use comma-separated list of addresses;
#defaults to 'localhost', and '*' is all ip address:
#listen_addresses='*'

import sys
import os
import re

OS=sys.argv[1]
username=sys.argv[2]
password=sys.argv[3]
network=sys.argv[4]

default=[
r'(local\s+all\s+all\s+)peer(.*)$',
r'(host\s+all\s+all\s+127\.0\.0\.1/32\s+)\S+(.*)$',
r'(host\s+all\s+all\s+\:\:1/128\s+)\S+(.*)$'
]
# The values as the database knows them can be canonically determined
# by the following:
# sudo -u postgres psql --no-psqlrc --no-align --quiet --tuples-only \
#   --command="SELECT setting FROM pg_settings WHERE name = 'hba_file'"
# sudo -u postgres psql -X -A -q -t \
#   -c "SELECT setting FROM pg_settings WHERE name = 'config_file';"
if OS in ['CentOS 6', 'CentOS 7', 'RedHat 6', 'RedHat 7']:
    pg_hba_conf="/var/lib/pgsql/9.3/data/pg_hba.conf"
    postgresql_conf="/var/lib/pgsql/9.3/data/postgresql.conf"
elif OS in ['Ubuntu 14.04']:
    pg_hba_conf="/etc/postgresql/9.3/main/pg_hba.conf"
    postgresql_conf="/etc/postgresql/9.3/main/postgresql.conf"
elif OS in ['Ubuntu 16.04']:
    pg_hba_conf="/etc/postgresql/9.5/main/pg_hba.conf"
    postgresql_conf="/etc/postgresql/9.5/main/postgresql.conf"
else:
    sys.stderr.write("Does not support %s\n" % OS)
    sys.exit(1)

results = []
with open(pg_hba_conf) as F:
    for line in F:
        line = line.rstrip()
        for pattern in default:
            m = re.search(pattern, line)
            if m:
                line = m.group(1) + 'trust' + m.group(2)
        results.append(line)
results.append('host    all    all    %s    md5' % network)
results.append('')

with open(pg_hba_conf, 'w') as W:
    W.write('\n'.join(results))

actual=open(postgresql_conf, 'r').readlines()
extra="listen_addresses='*'\n"
if actual[-1] != extra:
    actual.append(extra)
    open(postgresql_conf,'w').write(''.join(actual))
