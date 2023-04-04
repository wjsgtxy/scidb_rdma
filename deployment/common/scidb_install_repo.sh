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

function rhcommon ()
{
    yum -y makecache fast
    yum install --enablerepo=scidb3rdparty --enablerepo=cdash -y paradigm4-${scidb_version}-all paradigm4-${scidb_version}-dev-tools
}

function u_common ()
{
    apt-get update > /dev/null
    apt-get --allow-unauthenticated install -y paradigm4-${scidb_version}-all paradigm4-${scidb_version}-dev-tools
}

scidb_version=${1}

echo "Installing SciDB release..."
case $(./os_detect.sh) in
    "CentOS 6"|"RedHat 6")
	/sbin/chkconfig iptables off
	/sbin/service iptables stop
	rhcommon
	;;
    "CentOS 7"|"RedHat 7")
	/sbin/chkconfig firewalld off
	/sbin/service firewalld stop
	rhcommon
	;;
    "Ubuntu 14.04"|"Ubuntu 16.04")
        ufw disable
	u_common
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
echo "...installed SciDB release"
