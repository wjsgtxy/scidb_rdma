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

function centos6 ()
{
    yum -y makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-centos93-9.3-3.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-centos93-9.3-3.noarch.rpm
    yum install --enablerepo=scidb3rdparty -y $(ls *.rpm) || exit 1
}

function centos7 ()
{
    yum -y makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-centos93-9.3-3.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-centos93-9.3-3.noarch.rpm
    yum install --enablerepo=scidb3rdparty -y $(ls *.rpm) || exit 1
}

function redhat6 ()
{
    yum -y makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-redhat93-9.3-3.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-6-x86_64/pgdg-redhat93-9.3-3.noarch.rpm
    yum install --enablerepo=scidb3rdparty -y $(ls *.rpm) || exit 1
}

function redhat7 ()
{
    yum -y makecache fast
    yum install -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-redhat93-9.3-3.noarch.rpm || \
    yum update -y https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-redhat93-9.3-3.noarch.rpm
    yum install --enablerepo=scidb3rdparty -y $(ls *.rpm) || exit 1
}

function ubuntu ()
{
    function dependencies ()
    {
	(for package in $(ls *.deb | xargs); do
	    dpkg --info $package | grep Depends | sed -e "s/Depends://g" | sed -e "s/,/\n/g" | awk '{print $1}' | grep -v scidb;
	    dpkg --info $package | grep Depends | sed -e "s/Depends://g" | sed -e "s/,/\n/g" | awk '{print $1}' | grep scidb | grep libboost;
	done;) | sort -u
    }
    echo "Updating apt repositories..."
    apt-get update > /dev/null
    echo "Installing dependencies..."
    apt-get install --no-install-suggests --no-install-recommends -y $(dependencies) || exit 1
    dpkg -R -i . || exit 1

# remark: possible to use a method more like that of the centos6() function above? i dislike the current approach because
# it hard codes knowledge of specific 3rd party deps (libboost). Perhaps set up a Local Repository and then use
# apt-get install as usual? Also, investigate use of dpkg-scanpackages, which may prove helpful. jab 8/2013. See #3506
}

echo "Installing SciDB.."
case $(./os_detect.sh) in
    "RedHat 6")
	/sbin/chkconfig iptables off
	/sbin/service iptables stop
	redhat6
	;;
    "RedHat 7")
	/sbin/chkconfig iptables off
	/sbin/service iptables stop
	redhat7
	;;
    "CentOS 6")
	/sbin/chkconfig iptables off
	/sbin/service iptables stop
	centos6
	;;
    "CentOS 7")
	/sbin/chkconfig firewalld off
	/sbin/service firewalld stop
	centos7
	;;
    "Ubuntu 14.04"|"Ubuntu 16.04")
        ufw disable
	ubuntu
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
echo "...installed SciDB"
