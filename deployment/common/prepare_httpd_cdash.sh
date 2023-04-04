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
################################################################
# This will install, setup, and start apache2 web server
#
# This is used to serve out the CDash built package repositories.
#
# The packages are built in a hardcoded location as follows:
#   "${BUILD_VM_SOURCE_PATH_P4}_packages_${BUILD_TYPE}"
#   BUILD_VM_SOURCE_PATH_P4 is defined in <p4>/cdash2/control.config.*
#   and BUILD_TYPE is one of Assert, RelWithDebInfo, Valgrind
#
# For our purposes we create 3 symlinks in the /var/www directory
# corresponding to the 3 build types.

username=${1}
build_vm_source_path_p4=${2}

echo "Preparing httpd..."

case $(./os_detect.sh) in
    "CentOS 6"|"CentOS 7")
	yum -y makecache fast
	service iptables stop
	chkconfig iptables off
	yum install -y httpd
	chkconfig httpd on
	CONFIG=/etc/httpd/conf/httpd.conf
	cat ${CONFIG} | sed -e "s/\/var\/www\/html/\/var\/www/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	usermod -G apache -a ${username}
	chmod 755 /home/scidb
	ln -s ${build_vm_source_path_p4}_packages_Assert /var/www/p4_packages_Assert
	ln -s ${build_vm_source_path_p4}_packages_RelWithDebInfo /var/www/p4_packages_RelWithDebInfo
	ln -s ${build_vm_source_path_p4}_packages_Valgrind /var/www/p4_packages_Valgrind
	CONFIG=/etc/sysconfig/selinux
	cat ${CONFIG} | sed -e "s/enforcing/disabled/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	setenforce 0 || true
	service httpd start
	;;
    "RedHat 6"|"RedHat 7")
	echo "We do not support build SciDB under RedHat. Please use CentOS instead."
	exit 1
	;;
    "Ubuntu 14.04"|"Ubuntu 16.04")
	echo "Updating apt repositories..."
	apt-get update &> /dev/null
	echo "Installing apache2..."
	apt-get install -y -q apache2
	usermod -G www-data -a ${username}
	CONFIG=/etc/apache2/sites-available/000-default.conf
	cat ${CONFIG} | sed -e "s/\/var\/www\/html/\/var\/www/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	chmod 755 /home/scidb
	ln -s ${build_vm_source_path_p4}_packages_Assert /var/www/p4_packages_Assert
	ln -s ${build_vm_source_path_p4}_packages_RelWithDebInfo /var/www/p4_packages_RelWithDebInfo
	ln -s ${build_vm_source_path_p4}_packages_Valgrind /var/www/p4_packages_Valgrind
	;;
    *)
	echo "Not a supported OS"
	exit 1
	;;
esac

echo "...prepared httpd"
