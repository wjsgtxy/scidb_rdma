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

function detect ()
{
    local input="${1}"

    if   [ `echo "${input}" | grep "CentOS" | grep "[[:space:]]6\." | wc -l` = "1" ]; then
        OS="CentOS 6"
    elif [ `echo "${input}" | grep "CentOS" | grep "[[:space:]]7\." | wc -l` = "1" ]; then
        OS="CentOS 7"
    elif [ `echo "${input}" | grep "Ubuntu" | grep "14\.04" | wc -l` = "1" ]; then
        OS="Ubuntu 14.04"
    elif [ `echo "${input}" | grep "Ubuntu" | grep "16\.04" | wc -l` = "1" ]; then
        OS="Ubuntu 16.04"
    elif [ `echo "${input}" | grep "Red Hat" | grep "[[:space:]]6\." | wc -l` = "1" ]; then
        OS="RedHat 6"
    elif [ `echo "${input}" | grep "Red Hat" | grep "[[:space:]]7\." | wc -l` = "1" ]; then
        OS="RedHat 7"
    else
	OS="not supported"
    fi
}

if [ $# -eq 1 ]; then
    FILE=${1}
elif [ -f /etc/system-release ]; then
    FILE=/etc/system-release
elif [ -f /etc/os-release ]; then
    FILE=/etc/os-release
fi

PLATFORM=`cat ${FILE}`
detect "${PLATFORM}"

if [ "${OS}" != "not supported" ]; then
    echo "${OS}"
    exit 0
fi

PLATFORM=`lsb_release -d || cat ${FILE}`
detect "${PLATFORM}"

if [ "${OS}" == "not supported" ]; then
    echo "Not supported: "`echo ${PLATFORM} | head -n1`
    exit 1
fi

echo "${OS}"
exit 0
