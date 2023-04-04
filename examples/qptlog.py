#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2016-2019 SciDB, Inc.
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

"""
Given scidb.qpt TRACE logging enabled, analyze a scidb.log file
for queryPerThread foul-ups.

 1. Set "log4j.logger.scidb.qpt=TRACE" in .../share/scidb/log1.properties,
 2. Run some tests,
 3. Run this script on each instance's scidb.log file.  There should
    be no output.

"""

import argparse
import json
import os
import sys
import traceback

_args = None                    # Parsed arguments
_pgm = None                     # Program name


class AppError(Exception):
    """Base class for all exceptions that halt script execution."""
    pass


class Bunch(object):
    """See _Python Cookbook_ 2d ed., "Collecting a Bunch of Named Items"."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return self.__dict__.__str__()


def dbg(*args):
    if _args.verbose:
        print >>sys.stderr, _pgm, ' '.join(str(x) for x in args)


def warn(*args):
    print >>sys.stderr, _pgm, ' '.join(str(x) for x in args)


def prt(*args):
    print ' '.join(str(x) for x in args)


def parse_line(ll, lineno):
    """Parse a "queryPerThread" line from scidb.log"""
    result = Bunch()
    x = ll.split()
    result.tm = x[1]
    result.tid = x[2]
    result.obj = x[5][:-1]
    s = ' '.join(x[6:])
    try:
        aux = json.loads(s)
        result.__dict__.update(aux)
    except Exception as e:
        prt("Malformed JSON at line %d: %s" % (lineno, e))
        prt("Found in:", s)
    return result


class PutBackCheck(object):
    """Check that push/swapin and pop/swapout events match up.

    Every push/swapin must have a corresponding pop/swapout that puts
    back the old queryPerThread value.
    """
    def __init__(self):
        self.pushes = {}
        self.swapins = {}

    def __len__(self):
        r = len([x for x in self.pushes if self.pushes[x]])
        r += len([x for x in self.swapins if self.swapins[x]])
        return r

    def __call__(self, event):
        if event.op == 'push':
            tid = event.tid
            stack = self.pushes.setdefault(tid, [])
            stack.append(event)
        elif event.op == 'pop':
            stack = self.pushes[event.tid]
            assert stack[-1].old == event.qpt, "push/pop woes: %s !~ %s" % (
                stack[-1], event)
            self.pushes[event.tid].pop()
        elif event.op == 'swapin':
            tid = event.tid
            stack = self.swapins.setdefault(tid, [])
            stack.append(event)
        elif event.op == 'swapout':
            stack = self.swapins[event.tid]
            assert stack[-1].old == event.qpt, "swapin/out woes: %s !~ %s" % (
                stack[-1], event)
            self.swapins[event.tid].pop()


def process_one_file(file_handle, file_name):
    """Process an open input file."""
    lineno = 0
    pbc = PutBackCheck()
    for line in file_handle:
        lineno += 1
        if _args.logger not in line:
            continue
        x = parse_line(line, lineno)
        try:
            pbc(x)
        except AssertionError as e:
            prt("Line", lineno, "->", e)
    if len(pbc) != 0:
        warn("Dangling pushes:", pbc.pushes)
        warn("Dangling swapins:", pbc.swapins)


def process():
    """Process input files."""
    if _args.files_file:
        try:
            if _args.files_file == '-':
                FF = sys.stdin
            else:
                FF = open(_args.files_file)
            for fname in FF:
                fname = fname.strip()
                with open(fname) as F:
                    process_one_file(F, fname)
        finally:
            FF.close()
    elif not _args.files:
        process_one_file(sys.stdin, '(stdin)')
    else:
        for fname in _args.files:
            if fname == '-':
                process_one_file(sys.stdin, '(stdin)')
            else:
                with open(fname) as F:
                    process_one_file(F, fname)
    return 0


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by print

    parser = argparse.ArgumentParser(  # add_help=False,
        description="Fill in program description here!",
        epilog='Type "pydoc %s" for more information.' % _pgm[:-1])
    parser.add_argument('-f', '--files-file',
                        help="File containing list of input filenames")
    parser.add_argument('-l', '--logger', default='scidb.qpt',
                        help='Log4cxx logger name of messages to analyze')
    parser.add_argument('-v', '--verbose', default=0, action='count',
                        help='Debug logging level, 1=info, 2=debug, 3=debug+')
    parser.add_argument('files', nargs=argparse.REMAINDER,
                        help='The input file(s)')

    global _args
    _args = parser.parse_args(argv[1:])

    try:
        if _args.files_file and _args.files:
            raise AppError("Use -f/--files-file or provide file arguments,"
                           " but not both")
        return process()
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except KeyboardInterrupt:
        print >>sys.stderr, "^C"
        return 2
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2


if __name__ == '__main__':
    sys.exit(main())
