#!/usr/bin/python
#
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
#

'''
Run queries against the nyse_day array to see how favorite stocks are doing.
'''

import os
import sys
from nyse_tool import sym_to_id

DEFAULT_ARRAY = 'nyse_day'

# These are some of the most active stocks in our sample data's first
# hour of trading.
DEFAULT_FAVES = ['AIG', 'ANR', 'CNP', 'GE', 'HAL', 'JNPR', 'SSL', 'T', 'VR']


class Usage(Exception):
    pass


def run_query(symlist):
    '''Run interesting iquery...'''
    # Extra credit: figure out a way to do a single query for entire symlist.
    for sym in symlist:
        sym_id = sym_to_id(sym)
        query = '''aggregate(
                     apply(
                       slice({0}, symbolId, {1}),
                       fprice, price/pow(10,scale),
                       trade, 1),
                     min(symbol) as Symbol,
                     min(fprice) as Low,
                     max(fprice) as High,
                     sum(trade)  as NumTrades,
                     sum(volume) as Volume)'''.format(
            DEFAULT_ARRAY,
            sym_id)
        os.system('iquery -aq "%s"' % query)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    if len(argv) < 2:
        run_query(DEFAULT_FAVES)
    else:
        run_query(map(str.upper, argv[1:]))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Usage as e:
        print >>sys.stderr, e
        sys.exit(2)
