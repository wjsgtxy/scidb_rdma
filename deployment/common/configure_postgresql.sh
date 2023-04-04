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

set -eu

username=${1}
password="${2}"
network=${3}

if !(echo "${network}" | grep / 1>/dev/null); then
   echo "Invalid network format in ${network}"
   echo "Usage: configure_postgresql.sh network_ip (where network_ip=W.X.Y.Z/N) "
   exit 1;
fi

function postgresql_sudoers ()
{
    POSTGRESQL_SUDOERS=/etc/sudoers.d/postgresql
    echo "Defaults:${username} !requiretty" > ${POSTGRESQL_SUDOERS}
    echo "${username} ALL =(postgres) NOPASSWD: ALL" >> ${POSTGRESQL_SUDOERS}
    chmod 0440 ${POSTGRESQL_SUDOERS}
}

function rh6()
{
    yum install -y postgresql93 postgresql93-server postgresql93-contrib expect || \
    yum update -y postgresql93 postgresql93-server postgresql93-contrib expect
    /sbin/chkconfig postgresql-9.3 on
    su -l postgres -c "/usr/pgsql-9.3/bin/pg_ctl initdb"
    restart="service postgresql-9.3 restart"
    status="service postgresql-9.3 status"
}

function rh7()
{
    # systemctl #
    yum install -y postgresql93 postgresql93-server postgresql93-contrib expect || \
    yum update -y postgresql93 postgresql93-server postgresql93-contrib expect
    /bin/systemctl enable postgresql-9.3
    su -l postgres -c "/usr/pgsql-9.3/bin/pg_ctl initdb"
    restart="/bin/systemctl restart postgresql-9.3"
    status="/bin/systemctl status postgresql-9.3"
}

function ubuntu1404()
{
    echo "Updating apt repositories..."
    apt-get update &> /dev/null
    echo "Installing postgres packages..."
    apt-get install -y -q python-paramiko python-crypto postgresql-9.3 postgresql-contrib-9.3 expect
    restart="/etc/init.d/postgresql restart"
    status="/etc/init.d/postgresql status"
}

function ubuntu1604()
{
    # systemctl #
    echo "Updating apt repositories..."
    apt-get update &> /dev/null
    echo "Installing postgres packages..."
    apt-get install -y -q  python-crypto postgresql postgresql-contrib expect
    restart="/bin/systemctl restart postgresql"
    status="/bin/systemctl status postgresql"
}

OS=$(./os_detect.sh)
case ${OS} in
    "CentOS 6"|"RedHat 6")
	rh6
	;;
    "CentOS 7"|"RedHat 7")
	rh7
	;;
    "Ubuntu 14.04")
	ubuntu1404
	;;
    "Ubuntu 16.04")
	ubuntu1604
	;;
    *)
	echo "Not a supported OS";
	exit 1
esac;

postgresql_sudoers
./configure_postgresql.py "${OS}" "${username}" "${password}" "${network}" || echo "WARNING: failed to configure postgres !"
${restart}
${status}
