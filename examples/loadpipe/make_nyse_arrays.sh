:
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014-2019 SciDB, Inc.
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

# Create load array and target array for the NYSE loadpipe demo.

if [ "$1" = "-r" ]
then
    # remove 'em
    for ary in nyse_day nyse_flat nyse_flat2
    do
        iquery -aq "remove($ary)"
    done
else
    # Whitespace won't be preserved, but who cares.
    LOAD_SCHEMA="<
        mSeq     : int32,
        mType    : int32,
        otRef    : int32 null,
        srcSeq   : int32,
        srcSess  : int32,
        sendTime : int64,
        srcMsm   : int32,
        symbol   : string,
        price    : uint32,
        scale    : uint8,
        volume   : uint32,
        exchg    : string,
        secType  : string,
        linkId   : string,
        tc1      : string null,
        tc2      : string null,
        tc3      : string null,
        tc4      : string null,
        symbolId : int32
    > [i=0:*,1000000,0]"

    # The flat array for directly loading a TAQ file (as processed by nyse_feeder.py).
    # We create two for experiments with multiple loadpipe.py daemons.
    iquery -aq "create array nyse_flat $LOAD_SCHEMA ;" 1>/dev/tty
    iquery -aq "create array nyse_flat2 $LOAD_SCHEMA ;" 1>/dev/tty

    # Target array showing stock trades v. time.
    # 32400000000 == 9 * 60 * 60 * 1,000,000 == microseconds since midnight at 0900 hours.
    iquery -a 1>/dev/tty <<EOF
    create array nyse_day <
        symbol   : string,
        price    : uint32,
        scale    : uint8,
        volume   : uint32,
        exchg    : string
    > [symbolId=0:*, 500000, 0,
       sendTime=34200000000:*, 500000, 0];
EOF

fi
