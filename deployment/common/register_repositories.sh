#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2019-2019 SciDB, Inc.
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

VERSION=${1}
OS=$(./os_detect.sh)

cd /tmp
case ${OS} in
    "CentOS 6"|"RedHat 6"|"CentOS 7"|"RedHat 7")
        rm -f scidb-${VERSION}.0-devel-repos.rpm
        wget https://downloads.paradigm4.com/scidb-${VERSION}.0-devel-repos.rpm
        rpm -i scidb-${VERSION}.0-devel-repos.rpm
        rm -f scidb-${VERSION}.0-devel-repos.rpm
	;;
    "Ubuntu 14.04"|"Ubuntu 16.04")
        rm -f scidb-${VERSION}.0-dev-repos.deb
        wget https://downloads.paradigm4.com/scidb-${VERSION}.0-dev-repos.deb
        dpkg -i scidb-${VERSION}.0-dev-repos.deb
        rm -f scidb-${VERSION}.0-dev-repos.deb
	;;
esac
