#!/bin/bash
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
################################################################################
# This script sets up postgresql for use with SciDB
################################################################################
# Initialize postgresql database
#  This is only necessary (and only supported) on CentOS
#  The /dev/null and true is to not fail out if running on Ubuntu
service postgresql initdb > /dev/null 2>&1 || true

if [ -f /var/lib/pgsql/data/pg_hba.conf ]; then
    # Its a CentOS installation
    pg_hba_conf=/var/lib/pgsql/data/pg_hba.conf
    # Start postgresql on boot
    /sbin/chkconfig postgresql on
elif [ -f /etc/postgresql/*/main/pg_hba.conf ]; then
    # Its a Ubuntu installation
    pg_hba_conf=/etc/postgresql/*/main/pg_hba.conf
    # Start postgresql on boot
      # - its installed that way /etc/init.d/postgresql
    # Initialize postgresql database
      # - its installed initialized /var/lib/postgresql/*/main
else
    # Not a supported installation
    pg_hba_conf=/dev/null
fi

# Add scidb user to postgresql sudo file
echo "Defaults:scidb !requiretty" > /etc/sudoers.d/postgresql
echo "scidb ALL =(postgres) NOPASSWD: ALL" >> /etc/sudoers.d/postgresql
chmod 0440 /etc/sudoers.d/postgresql

# change authentication to md5 for local (127.0.0.1) ports
sed -i 's/^\(host\s\+all\s\+all\s\+127.0.0.1\/32\s\+\)\(ident\|peer\)$/\1md5/' $pg_hba_conf

# remove IP6 support
sed -i '/^\(host\s\+all\s\+all\s\+::1\/128\s\+\)/d' $pg_hba_conf

# Restart postgrsql
if [ -f /var/lib/pgsql/data/pg_hba.conf ]; then
    # Its a CentOS installation
    service postgresql restart
elif [ -f /etc/postgresql/*/main/pg_hba.conf ]; then
    /etc/init.d/postgresql restart
else
    :
fi
