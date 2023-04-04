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

username=${1}
password="${2}"
network=${3}

if !(echo "${network}" | grep / 1>/dev/null); then
   echo "Invalid network format in ${network}"
   echo "Usage: start_postgres.sh network_ip (where network_ip=W.X.Y.Z/N) "
   exit 1;
fi

function postgresql_sudoers ()
{
    POSTGRESQL_SUDOERS=/etc/sudoers.d/postgresql
    echo "Defaults:${username} !requiretty" > ${POSTGRESQL_SUDOERS}
    echo "${username} ALL =(postgres) NOPASSWD: ALL" >> ${POSTGRESQL_SUDOERS}
    chmod 0440 ${POSTGRESQL_SUDOERS}
}

function rhcommon6()
{
    /sbin/chkconfig postgresql-9.3 on
    su -l postgres -c "/usr/pgsql-9.3/bin/pg_ctl initdb"
    restart="service postgresql-9.3 restart"
    status="service postgresql-9.3 status"
}

function ubuntu1404()
{
    restart="/etc/init.d/postgresql restart"
    status="/etc/init.d/postgresql status"
}

function ubuntu1604()
{
    # systemctl #
    /bin/systemctl enable postgresql
    restart="/bin/systemctl restart postgresql"
    status="/bin/systemctl status postgresql"
}

OS=$(lsb_release -i | cut -d ":" -f 2 | cut -c 1-7 | tr -d '[:space:]')
OS="$OS $(lsb_release -r | cut -d ":" -f 2 | tr -d '[:space:]')"

case ${OS} in
    CentOS*|RedHat*)
	OS=$(echo $OS | cut -d . -f 1)
	rhcommon6
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
