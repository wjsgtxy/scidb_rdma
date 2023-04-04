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

function centos ()
{
    yum clean all > /dev/null
    yum list | grep paradigm4-${release} | awk '{print $1}' | grep '[a-zA-Z]' | xargs rpm -e
    yum list | grep scidb-${release} | awk '{print $1}' | grep '[a-zA-Z]' | xargs rpm -e
    yum -y makecache fast
}

function ubuntu ()
{
    apt-get update > /dev/null
    apt-get clean > /dev/null
    dpkg --list | grep paradigm4-${release} | awk '{print $2}' | xargs dpkg --purge
    dpkg --list | grep scidb-${release} | awk '{print $2}' | xargs dpkg --purge
    apt-get autoremove --purge -y
    apt-get update > /dev/null
}

release=${1}

case $(./os_detect.sh) in
    "CentOS 6"|"RedHat 6"|"CentOS 7"|"RedHat 7")
	centos
	;;
    "Ubuntu 14.04"|"Ubuntu 16.04")
	ubuntu
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
