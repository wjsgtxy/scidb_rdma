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
################################################################################
# This script setups and runs SciDB.
################################################################################
cat /opt/scidb/VERSION/script/config.ini > /opt/scidb/VERSION/etc/config.ini
chown scidb /opt/scidb/VERSION/etc/config.ini

sudo -u postgres /opt/scidb/VERSION/bin/scidbctl.py init_syscat single_server
su -l scidb -c '/opt/scidb/VERSION/bin/scidbctl.py init-cluster --force single_server'

su -l scidb -c '/opt/scidb/VERSION/bin/scidbctl.py start single_server'
