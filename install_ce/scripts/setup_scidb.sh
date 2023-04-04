#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2016-2019 SciDB, Inc.
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

if [ $# -ne 1 ]; then
    echo "Please pass in SciDB's installation directory."
    exit 1
fi

ipaddr=$(ip -o address | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | head -n1)
ipaddr=$(echo $ipaddr | awk 'BEGIN {FS="."} {print $1"."$2"."$3".0/32"}')

./start_postgres.sh "scidb_pg_user" "scidb_pg_user_pasw" $ipaddr

if [ ! -e /home/$(logname)/.ssh/id_rsa.pub ]; then
    sudo -u $(logname) ssh-keygen -t rsa -N "" -f /home/$(logname)/.ssh/id_rsa
fi
sudo -u $(logname) sh -c "cat /home/$(logname)/.ssh/id_rsa.pub >> /home/$(logname)/.ssh/authorized_keys"
sudo -u $(logname) sh -c "chmod 600 /home/$(logname)/.ssh/authorized_keys"
if [ -x /sbin/restorecon ]; then
    /sbin/restorecon /home/$(logname)/.ssh /home/$(logname)/.ssh/authorized_keys
fi

sudo -u postgres $1/bin/scidbctl.py init-syscat --db-password "scidb_pg_user_pasw" mydb

sudo -u $(logname) sh -c "echo "127.0.0.1:5432:mydb:scidb_pg_user:scidb_pg_user_pasw" >> /home/$(logname)/.pgpass"
sudo -u $(logname) chmod 600 /home/$(logname)/.pgpass

sed -i "s^BASEPATH^/home/$(logname)/scidb_data^" $1/etc/config.ini

sed -i "s/log4j.rootLogger=DEBUG, file/log4j.rootLogger=INFO, file/" $1/share/scidb/log4cxx.properties

sudo -u $(logname) $1/bin/scidbctl.py init-cluster --force mydb
sudo -u $(logname) $1/bin/scidbctl.py start mydb
