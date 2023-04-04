#!/bin/bash

##
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
##

#Load the sample data file into the array trades_flat.
#This file happens to be a sample of stock trade data, for a few selected stocks, downloaded from NYSE.

ARRAY="trades_flat"
SCHEMA='<symbol:string, ms:int64, volume:uint64, price:double>[i=0:*,1000000,0]'
TMPDIR=$(mktemp -d /tmp/ldtr.XXXXXXXXXX)

trap "rm -rf $TMPDIR" 15 3 2 1 0

mkfifo $TMPDIR/fifo

iquery -aq "remove(trades_flat)" 2>/dev/null
iquery -aq "create array trades_flat $SCHEMA"

zcat trades_small.csv.gz | sed 1d > $TMPDIR/fifo &
iquery -naq "load($ARRAY, '"$TMPDIR/fifo"', -2, 'csv')"
wait
