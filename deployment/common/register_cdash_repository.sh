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
# This will create the yum/apt repository url file
# used to find the CDash package respositories that were setup
# in the prepare_httpd_cdash.sh script in this directory.
# The locations, p4_packages_..., are symlinked in that same script.

function centos()
{
(cat <<EOF
[cdash]
name=CDash repository
baseurl=http://${repo_url}/p4_packages_${build_type}/
gpgcheck=0
enabled=0
EOF
) > /etc/yum.repos.d/cdash.repo
    yum clean all
    yum -y makecache fast
}

function ubuntu()
{
    echo "deb http://${repo_url}/ p4_packages_${build_type}/" > /etc/apt/sources.list.d/cdash.list
    apt-get update &> /dev/null
}

repo_url=${1}
build_type=${2}

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
