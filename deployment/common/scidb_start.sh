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

set -u

database=${1}
SCIDB_VER="${2}"
shift 2

if [ $# -ne 0 ]; then
    /opt/scidb/${SCIDB_VER}/bin/scidbctl.py --auth-file ${1} start ${database}
    rc=$?
else
    /opt/scidb/${SCIDB_VER}/bin/scidbctl.py start ${database}
    rc=$?
fi
if [ $rc -eq 0 ]; then
    if [ $# -ne 0 ]; then
        until /opt/scidb/${SCIDB_VER}/bin/iquery --auth-file ${1} -aq "list()" > /dev/null 2>&1; do sleep 1; done
    else
        until /opt/scidb/${SCIDB_VER}/bin/iquery -aq "list()" > /dev/null 2>&1; do sleep 1; done
    fi
fi

