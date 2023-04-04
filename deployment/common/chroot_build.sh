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

case $(./os_detect.sh) in
    "CentOS 6")
	./chroot_build.py -i -d centos-6-x86_64 -t .
	./chroot_build.py -d centos-6-x86_64 --exe --command "rpm -i https://repo.saltstack.com/yum/redhat/salt-repo-latest-2.el6.noarch.rpm"
	;;
    "CentOS 7")
	./chroot_build.py -i -d centos-7-x86_64 -t .
	exit 1
	;;
    "RedHat 6"|"RedHat 7")
	echo "We do not support build SciDB under RedHat. Please use CentOS instead"
	exit 1
	;;
    "Ubuntu 14.04")
	./chroot_build.py -i -d ubuntu-trusty-amd64 -t .
	;;
    "Ubuntu 16.04")
	./chroot_build.py -i -d ubuntu-xenial-amd64 -t .
	;;
    *)
	echo "Not a supported OS"
	exit 1
	;;
esac
